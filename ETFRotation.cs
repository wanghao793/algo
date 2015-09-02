/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/


/*
 * To add RollingWindow<TradeBar> for each symbol, to record a fixed length of day bars
 * To add Day consolidator, which will fire events daily to update RollingWindow
 * in OnData(minute level), judge if trading time, if so, update correlation of each symbol.
 * 
 * then calculate rank of each factor and compute composite score, and rotate
 * 
 * when rotate, do not liquidate all at the begining, instead, write a order target functioin.
 * 
*/

using System;
using System.Collections.Generic;
using System.Linq;
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Indicators;
using QuantConnect.Orders;
using QuantConnect.Data.Consolidators;
using MathNet.Numerics.Statistics;
using Accord.Statistics;

namespace QuantConnect.Algorithm.Examples
{
    /// <summary>
    /// ETF Global Rotation Strategy
    /// </summary>
    public class ETFRotation : QCAlgorithm
    {
        // we'll use this to tell us when the month has ended
        public DateTime LastRotationTime = DateTime.MinValue;
        public readonly TimeSpan RotationInterval = TimeSpan.FromDays(28);
        //to tell if the first trade--may be retired later, since robust algorithm should be OK with accounts with holdings already
        private bool first = true;
        //How to consolidate the bar, which will be saved in SymbolData._history(RollingWindow), for other calculations, e.g. Correlation Matrix
        public readonly TimeSpan BarResolution = TimeSpan.FromDays(1);
        //How many bars to be stored in the _history
        public readonly int LookBackPeriod_Day = 120;
        //Symbol Keyed SymbolData
        public readonly Dictionary<string, SymbolData> Data = new Dictionary<string, SymbolData>();
        public readonly double WA = 1, WB = 1, WC = 0.5;
        //Update Status, once used, the status will be set False again; before each rotation, wait until all status are True
        public Dictionary<string, bool> UpdateStatus = new Dictionary<string, bool>();
        // these are the growth symbols we'll rotate through
        public IReadOnlyList<string> GrowthSymbols = new List<string>
        {
            "MDY", // US S&P mid cap 400
            "IEV", // iShares S&P europe 350
            "EEM", // iShared MSCI emerging markets
            "ILF", // iShares S&P latin america
            "EPP"  // iShared MSCI Pacific ex-Japan
        };

        // these are the safety symbols we go to when things are looking bad for growth
        public IReadOnlyList<string> SafetySymbols = new List<string>
        {
            "EDV", // Vangaurd TSY 25yr+
            "SHY"  // Barclays Low Duration TSY
        };

        /// <summary>
        /// Initialise the data and resolution required, as well as the cash and start-end dates for your algorithm. All algorithms must initialized.
        /// </summary>
        public override void Initialize()
        {
            SetCash(25000);
            SetStartDate(2007, 1, 1);

            //UpdateStatus = new List<bool>(Enumerable.Repeat(false,GrowthSymbols.Union(SafetySymbols).ToList().Count()));

            foreach (var symbol in GrowthSymbols.Union(SafetySymbols))
            {
                Data.Add(symbol, new SymbolData(symbol, SecurityType.Equity, BarResolution, LookBackPeriod_Day));
                UpdateStatus[symbol] = false;
            }

            foreach (var kvp in Data)
            {
                // this is required since we're using closures below, for more information
                // see: http://stackoverflow.com/questions/14907987/access-to-foreach-variable-in-closure-warning
                var symbolData = kvp.Value;
                //Minute Level subscriptions
                AddSecurity(SecurityType.Equity, symbolData.Symbol, Resolution.Daily);
                //Construct daily Momentum and SD indication by using Helper function, in which auto daily updates is handled by RegisterIndicator function
                //But this is only litmited to case when Resolution.XXX is ready. Otherwise, e.g. 10minutes bar, separate update function need be created, updated by Indicator's Update functions
                symbolData.Return = new Momentum(CreateIndicatorName(symbolData.Symbol, "MOM" + LookBackPeriod_Day, Resolution.Minute), LookBackPeriod_Day);
                symbolData.SD = new StandardDeviation(CreateIndicatorName(symbolData.Symbol, "SMA" + LookBackPeriod_Day, Resolution.Minute), LookBackPeriod_Day);
                STD(symbolData.Symbol, LookBackPeriod_Day, Resolution.Daily);
                //Daily Level Consolidator for storing history data to Bars
                var consolidator = new TradeBarConsolidator(BarResolution);
                consolidator.DataConsolidated += (sender, bar) => //we may also update Return and SD in this event handler, put Bars as the bottom, such that if Bar is updated, then the other must have been updated
                {
                    symbolData.Return.Update(bar.Time, bar.Value);
                    symbolData.SD.Update(bar.Time, bar.Value);
                    symbolData.Bars.Add(bar);//once bar updated, the return and sd must be updated
                };
                SubscriptionManager.AddConsolidator(symbolData.Symbol, consolidator);
            }

        }


        /// <summary>
        /// OnData event is the primary entry point for your algorithm. Each new data point will be pumped in here.
        /// </summary>
        /// <param name="data">TradeBars IDictionary object with your stock data</param>
        public override void OnData(Slice data)
        {
            try
            {
                // the first time we come through here we'll need to do some things such as allocation
                // and initializing our symbol data
                if (first)
                {
                    first = false;
                    LastRotationTime = data.Time;
                    return;
                }

                var delta = data.Time.Subtract(LastRotationTime);
                //Debug(delta.ToString());
                if (delta > RotationInterval)
                {
                    //First check if all symboldata are ready and were just updated
                    foreach (var kvp in Data)
                    {
                        var symbolData = kvp.Value;
                        //only update when status is false, it will be set back to false when all securities are ready
                        if (!UpdateStatus[symbolData.Symbol]) UpdateStatus[symbolData.Symbol] = symbolData.IsReady && symbolData.WasJustUpdated(data.Time);
                        //Debug(symbolData.Symbol+symbolData.Bars.Count.ToString());
                        //Debug(symbolData.Symbol + symbolData.SD.Samples.ToString());
                        //Debug(symbolData.Symbol + symbolData.Return.Samples.ToString());
                    }
                    //if any status is false return and wait for all updated, otherwise reset all status to false and proceeed with rotation;
                    if (UpdateStatus.Values.Any(x => x == false)) return;
                    else { foreach (var key in UpdateStatus.Keys.ToList()) UpdateStatus[key] = false; }

                    //if all updated, update rotation time
                    LastRotationTime = data.Time;
                    //prepare symbols and size for index based operation (instead of sequence based)
                    var allSymbols = Data.Keys.ToList();
                    int nSymbols = allSymbols.Count();
                    // store history matrix and calculate correlation matrix
                    double[,] history_matrix = new double[LookBackPeriod_Day, nSymbols];
                    for (int i = 0; i < nSymbols; i++)
                    {
                        var symbol = allSymbols[i];
                        var sData = Data[symbol];
                        for (int j = 0; j < LookBackPeriod_Day; j++)
                        {
                            history_matrix[j, i] = (double)sData.Bars[LookBackPeriod_Day - j - 1].Close;
                        }
                    }

                    var corr_mat = Tools.Correlation(history_matrix);
                    //calculate correlation score for each symbol and the rank
                    for (int i = 0; i < nSymbols; i++)
                    {
                        double temp = 0;
                        for (int j = 0; j < nSymbols; j++) temp += corr_mat[i, j];
                        Data[allSymbols[i]].CORR = (decimal)temp;
                    }
                    //calculate the ranked Return/SD/CORR and calculate the multifactor score
                    Data.Select(x => (double)x.Value.WeightedReturn).ToList().Ranks();

                    var rank_Return = Data.Select(x => (double)x.Value.WeightedReturn).ToList().Ranks();
                    var rank_SD = Data.Select(x => 1.0 / (double)x.Value.SD.Current.Value).ToList().Ranks();
                    var rank_CORR = Data.Select(x => 2.0 - (double)x.Value.CORR).ToList().Ranks();//to check the exact formula
                    var weightedScore = rank_Return.Zip(rank_SD, (x, y) => WA * x + WB * y).Zip(rank_CORR, (x, y) => x + WC * y);
                    var orderedSymbolScores = allSymbols.Zip(weightedScore, (x, y) => new { symbol = x, score = y }).OrderByDescending(x => x.score);

                    foreach (var orderedSymbolScore in orderedSymbolScores)
                    {
                        Log(">>SCORE>>" + orderedSymbolScore.symbol + ">>" + orderedSymbolScore.score);
                    }
                    //consider best one only, can extend to multiple securities
                    var bestGrowth = orderedSymbolScores.First();

                    if (Data[bestGrowth.symbol].WeightedReturn > 0) //absolute return must be >0, otherwise, put all into cash/cash etf
                    {
                        if (Portfolio[bestGrowth.symbol].Quantity == 0)
                        {
                            Log("PREBUY>>LIQUIDATE>>");
                            Liquidate();
                        }
                        Log(">>BUY>>" + bestGrowth.symbol + "@" + (100 * Data[bestGrowth.symbol].Return.Current.Value).ToString("00.00"));
                        decimal qty = Portfolio.TotalPortfolioValue / Securities[bestGrowth.symbol].Close;
                        Order(bestGrowth.symbol, qty, OrderType.Market);
                    }
                    else
                    {
                        // if no one has a good objective score then let's hold cash this month to be safe
                        Log(">>LIQUIDATE>>CASH");//may change to cash etf
                        Liquidate();
                    }
                    Plot("Best Return", Data[bestGrowth.symbol].WeightedReturn);
                }
            }
            catch (Exception ex)
            {
                Error("OnTradeBar: " + ex.Message + "\r\n\r\n" + ex.StackTrace);
            }
        }
    }

    public class SymbolData
    {
        public readonly string Symbol;
        public readonly SecurityType SecurityType;

        public RollingWindow<TradeBar> Bars;
        public TimeSpan BarResolution;

        public Momentum Return { get; set; }
        public StandardDeviation SD { get; set; }
        public decimal? CORR { get; set; }

        public SymbolData(string symbol, SecurityType securityType, TimeSpan barResolution, int lookBackPeriod_Day)
        {
            Symbol = symbol;
            SecurityType = securityType;
            BarResolution = barResolution;
            Bars = new RollingWindow<TradeBar>(lookBackPeriod_Day);
            CORR = null;
        }

        public decimal WeightedReturn /*used as absolute momentum*/
        {
            get
            {
                // we weight the one month performance higher
                decimal weight1 = 100m;
                //decimal weight2 = 75;
                //decimal weight3 = 75;

                return (weight1 * Return /*+ weight2 * ThreeMonthRETURN + weight3 * SixMonthRETURN*/) / (weight1 /*+ weight2 + weight3*/);
            }
        }

        public bool IsReady
        {
            get { return Bars.IsReady && Return.IsReady && SD.IsReady; }
        }

        public bool WasJustUpdated(DateTime current)
        {
            return Bars.Count > 0 && Bars[0].EndTime == current - BarResolution;

        }

    }
}