package com.nexcorio.algo.util;

/**
 * 
 * @author Keshav Shetty
 */
import java.lang.Math;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.kite.ZerodhaIntradayStreamingThread;

public class BSOption {
	
	private static final Logger log = LogManager.getLogger(BSOption.class);
	private static double INTEREST_RATE = 0.1; // 10% is interest rate

	public double S;        // underlying asset price
	public double K;        // strike price option
	public double T;        // time to maturity option
	public double r;        // annual risk-free rate
	public double q;        // annual yield rate asset
	public double vol;      // underlying asset volatility
	public double price;    // price of option (if given)
	public String type;
	
	
	public static void main(String[] args) {
		
		double interestRate = 0.1; // 10% is interest rate
		
		double stockPrice = 17553.5; // Spotprice
		double strikePrice = 17550;
		double volatality = 15.85f/100f; // Nse site 9.12 so devide by 100
		
		
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 15);
		cal.set(Calendar.MINUTE, 29);
		Date currentTime = cal.getTime();
		
		cal.set(Calendar.DATE, 23);
		cal.set(Calendar.MINUTE, 30);
		Date expireTime = cal.getTime();
		
		System.out.println("currentTime "+currentTime+" expireTime=" + expireTime);
		long diffInMillies = Math.abs(currentTime.getTime() - expireTime.getTime());
		
		float fractioAsDayinYears = ((float)diffInMillies)/(1000f*60f*60f*24f*365f);
		
		BSOption aBs = new BSOption(stockPrice, strikePrice, fractioAsDayinYears, interestRate, 0, volatality, 0, "CE");
		System.out.println(aBs.computePrice());
		double[] greeks = aBs.computeGreeks();
		System.out.println("Delta="+greeks[0]+" Vega="+greeks[1]+" Psi="+greeks[2]+" Theta="+greeks[3]);
		System.out.println(" Rho="+greeks[4]+" Gamma="+greeks[5]+" Volga="+greeks[6]);
	    
	}
	
	// default constructor
	BSOption(){}
	
	// defined constructor
	public BSOption(double s, double k, double t, double r, double q, double vol, double price, String type) {
	   this.S = s;
	   this.K = k;
	   this.T = t;
	   this.r = r;
	   this.q = q;
	   this.vol = vol;
	   this.price = price;
	   this.type = type;
	}
	
	public double computePrice() {
	   double dplus = (Math.log(S / K) + (r - q + Math.pow(vol,2) / 2) * T) / (vol * Math.sqrt(T));
	   double dminus = dplus - vol * Math.sqrt(T);
	   double price;
	   if (type.equals("CE")) price = S * CND(dplus) * Math.exp(-q * T) - K * CND(dminus) * Math.exp(-r * T); // price of a call option
	   else price = S * -1 * CND(-dplus) * Math.exp(-q * T) + K * CND(-dminus) * Math.exp(-r * T);             // price of a put option
	   return price;
	}
	
	public double computePriceDiagram (double spot, double t) {
	   double dplus = (Math.log(spot / K) + (r - q + Math.pow(vol,2) / 2) * (T-t)) / (vol * Math.sqrt((T-t)));
	   double dminus = dplus - vol * Math.sqrt((T-t));
	   double price;
	   if (type.equals("CE")) price = spot * CND(dplus) * Math.exp(-q * (T-t)) - K * CND(dminus) * Math.exp(-r * (T-t)); // price of a call option
	   else price = spot * -1 * CND(-dplus) * Math.exp(-q * (T-t)) + K * CND(-dminus) * Math.exp(-r * (T-t));             // price of a put option
	   return price;
	}
	
	public double computePriceVol(double v) {
	   double dplus = (Math.log(S / K) + (r - q + Math.pow(v,2) / 2) * T) / (v * Math.sqrt(T));
	   double dminus = dplus - v * Math.sqrt(T);
	   double price;
	   if (type.equals("CE")) price = S * CND(dplus) * Math.exp(-q * T) - K * CND(dminus) * Math.exp(-r * T); // price of a call option
	   else price = S * -1 * CND(-dplus) * Math.exp(-q * T) + K * CND(-dminus) * Math.exp(-r * T);             // price of a put option
	   return price;
	}
	
	public double computePriceVolStrike(double v, double strike, double t) {
	   double dplus = (Math.log(S / strike) + (r - q + Math.pow(v,2) / 2) * (T-t)) / (v * Math.sqrt(T-t));
	   double dminus = dplus - v * Math.sqrt(T-t);
	   double priceRes;
	   if (type.equals("CE")) priceRes = S * CND(dplus) * Math.exp(-q * (T-t)) - strike * CND(dminus) * Math.exp(-r * (T-t)); // price of a call option
	   else priceRes = S * -1 *CND(-dplus) * Math.exp(-q * (T-t)) + strike * CND(-dminus) * Math.exp(-r * (T-t));             // price of a put option
	   return priceRes;
	}
	
	public double[] computeGreeks() {
	   double dplus = (Math.log(S / K) + (r - q + Math.pow(vol,2) / 2) * T) / (vol * Math.sqrt(T));
	   double dminus = dplus - vol * Math.sqrt(T);
	   // compute Delta, Vega, Psi, Theta, Rho, Gamma and Volga
	   double[] greeks = new double[7];
	   if (type.equals("CE")) {
	      // Delta
	      greeks[0] = Math.exp(-q*T)*CND(dplus);
	      // Vega
	      greeks[1] = S*Math.exp(-q*T)*ND(dplus)*Math.sqrt(T);
	      // Psi
	      greeks[2] = -S*Math.exp(-q*T)*(Math.sqrt(T)/vol*ND(dplus)+T*CND(dplus))+K*Math.exp(-r*T)*ND(dminus)*Math.sqrt(T)/vol;
	      // Theta
	      greeks[3] = -S*ND(dplus)*vol/(2*Math.sqrt(T))-r*K*Math.exp(-r*T)*CND(dminus)+q*S*Math.exp(-q*T)*CND(dplus);
	      // Rho
	      greeks[4] = K*T*Math.exp(-r*T)*CND(dminus);
	      // Gamma
	      greeks[5] = Math.exp(-q*T)*ND(dplus)/(S*vol*Math.sqrt(T)) ;
	      // Volga (use vega)
	      greeks[6] = greeks[1]*dplus*dminus/vol;
	   }
	   else {
	      // Delta
	      greeks[0] = -Math.exp(-q*T)*CND(-dplus);
	      // Vega
	      greeks[1] = S*Math.exp(-q*T)*ND(dplus)*Math.sqrt(T);
	      // Psi
	      greeks[2] = S*Math.exp(-q*T)*(-Math.sqrt(T)/vol*ND(-dplus)+T*CND(-dplus))+K*Math.exp(-r*T)*ND(-dminus)*Math.sqrt(T)/vol;
	      // Theta
	      greeks[3] = -S*ND(dplus)*vol/(2*Math.sqrt(T))+r*K*Math.exp(-r*T)*CND(-dminus)-q*S*Math.exp(-q*T)*CND(-dplus);
	      // Rho
	      greeks[4] = -K*T*Math.exp(-r*T)*CND(-dminus);
	      // Gamma
	      greeks[5] = Math.exp(-q*T)*ND(dplus)/(S*vol*Math.sqrt(T)) ;
	      // Volga (use vega)
	      greeks[6] = greeks[1]*dplus*dminus/vol;
	   }
	
	   return greeks;
	}
	
	// Determine implied vol of a call using Newton-Raphson root finding algorithm
	
	public double ImpliedVol_NewtonRaphson(double spotPrice, double guess, int maxIter, double epsilon) {
	   int i = 0;
	   double X2 = guess;
	   double p = computePriceVol(X2) - spotPrice;
	   while (i < maxIter && Math.abs(p) > epsilon) {
	      double dplus = (Math.log(S / K) + (r - q + Math.pow(X2,2) / 2) * T) / (X2* Math.sqrt(T));
	      X2 = X2 - (computePriceVol(X2) - spotPrice) / (S*Math.exp(-q*T)*ND(dplus)*Math.sqrt(T)); // divide by vega (the derivative of price regaring volatility)
	      p = computePriceVol(X2) - spotPrice;
	      i++;
	   }
	   return X2;
	}
	
	public double ImpliedVol_Secant(double spotPrice, double guess1, double guess2, int maxIter, double epsilon) {
	   int i=0;
	   double X1 = guess1;
	   double X2 = guess2;
	   double p = computePriceVol(X2) - spotPrice;
	   while (i < maxIter && Math.abs(p) > epsilon) {
	      double temp = X2;
	      X2 = X2 - (computePriceVol(X2) - spotPrice) * (X2 - X1) / (computePriceVol(X2)-computePriceVol(X1));
	      p = computePriceVol(X2) - spotPrice;
	      X1 = temp;
	      i++;
	   }
	   return X2;
	}
	
	public double ImpliedVol_RegulaFalsi(double spotPrice, double guess1, double guess2, int maxIter, double epsilon) {
	   int i=0;
	   double X1 = guess1;
	   double X2 = guess2;
	   double p = computePriceVol(X2) - spotPrice;
	   while (i < maxIter && Math.abs(p) > epsilon) {
	      double bk = X2;
	      X2 = X2 - (computePriceVol(X2) - spotPrice) * (X2 - X1) / (computePriceVol(X2)-computePriceVol(X1));
	      p = computePriceVol(X2) - spotPrice;
	      if (p * (computePriceVol(bk) - spotPrice) >0 ) {
	         X1 = X2;
	         X2 = bk;
	      }
	      i++;
	   }
	   return X2;
	}
	
	// used for implied vol for volatility smile
	public double ImpliedVol_NewtonRaphsonSmile(double spotPrice, double strike, double t, double guess, int maxIter, double epsilon) {
	   int i = 0;
	   double X2 = guess;
	   double p = computePriceVolStrike(X2, strike, t) - spotPrice;
	   while (i < maxIter && Math.abs(p) > epsilon) {
	      double dplus = (Math.log(S / strike) + (r - q + Math.pow(X2,2) / 2) * (T-t)) / (X2* Math.sqrt((T-t)));
	      X2 = X2 - (computePriceVolStrike(X2, strike, t) - spotPrice) / (S*Math.exp(-q*(T-t))*ND(dplus)*Math.sqrt(T-t)); // divide by vega (the derivative of price regarding volatility)
	      p = computePriceVolStrike(X2, strike, t) - spotPrice;
	      i++;
	   }
	   return X2;
	}
	
	public double ImpliedVol_SecantSmile(double spotPrice, double strike, double t, double guess1, double guess2, int maxIter, double epsilon) {
	   int i=0;
	   double X1 = guess1;
	   double X2 = guess2;
	   double p = computePriceVolStrike(X2, strike, t) - spotPrice;
	   while (i < maxIter && Math.abs(p) > epsilon) {
	      double temp = X2;
	      X2 = X2 - (computePriceVolStrike(X2, strike, t) - spotPrice) * (X2 - X1) / (computePriceVolStrike(X2, strike, t)-computePriceVolStrike(X1, strike, t));
	      p = computePriceVolStrike(X2, strike, t) - spotPrice;
	      X1 = temp;
	      i++;
	   }
	   return X2;
	}
	
	
	// other helpers functions
	public static double ND(double x) {
	   return 1/Math.sqrt(2 * Math.PI) * Math.exp(-Math.pow(x,2)/2);
	}
	
	public static double CND(double x) {
	
	   // coefficients obtained
	   double a1 = 0.31938153;
	   double a2 = -0.356563782;
	   double a3 = 1.781477937;
	   double a4 = -1.821255978;
	   double a5 = 1.330274429;
	
	   double L = Math.abs(x);
	   double K = 1 / (1 + 0.2316419 * L);
	   double res = 1 - 1 / Math.sqrt(2 * Math.PI) * Math.exp(-Math.pow(L,2) / 2) * (a1 * K + a2 * Math.pow(K,2) + a3 * Math.pow(K,3) + a4 * Math.pow(K,4) + a5 * Math.pow(K,5));
	   if (x<0)
	      // if x negative, then reverse approximation by applying 1-x
	      res = 1 - res;
	   return res;
	}
	
	public float guessTheIV(double optionPrice, double underlyingValue, double strikePrice, String optionType, Date expDate) {
		float retVal = 0f;
		try {
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(expDate);
			cal.set(Calendar.HOUR_OF_DAY, 15);
			cal.set(Calendar.MINUTE, 30);
			//System.out.println("for "+expDate+" caltime=" + cal.getTime());
			
			long diffInMillies = Math.abs(cal.getTimeInMillis() - (new Date()).getTime());
			
			float fractioAsDayinYears = ((float)diffInMillies)/(1000f*60f*60f*24f*365f);
			
			double upperIV = 100d;
			double lowerIV = 1d;
			
			BSOption upperIVBS = new BSOption(underlyingValue, strikePrice, fractioAsDayinYears, INTEREST_RATE, 0f, upperIV/100f, 0f, optionType);
			double upperIVPrice = upperIVBS.computePrice();
			//System.out.println("upperIV="+upperIVPrice);
					
			BSOption lowerIVBS = new BSOption(underlyingValue, strikePrice, fractioAsDayinYears, INTEREST_RATE, 0f, lowerIV/100f, 0f, optionType);
			double lowerIVPrice = lowerIVBS.computePrice();
			//System.out.println("lowerIVBS="+lowerIVPrice);
			
			double midPointPrice = 0f;
			double midPoint = 0f;
			int iterCount = 0;
			
			do {
				iterCount++;
				midPoint = (upperIV+lowerIV)/2d; 
				BSOption midPointPriceIV = new BSOption(underlyingValue, strikePrice, fractioAsDayinYears, INTEREST_RATE, 0f, midPoint/100f, 0f, optionType);
				midPointPrice = midPointPriceIV.computePrice();
				//System.out.println(iterCount+" midPoint="+ midPoint + " midPointIVBS="+midPointPrice);
				
				if (optionPrice>midPointPrice) {
					lowerIV = midPoint;
				} else {
					upperIV = midPoint;
				}
				if (iterCount>20) {
					retVal = 0;
					break;
				} else {
					retVal = (float) midPoint;
				}
			} while(Math.abs(midPointPrice-optionPrice)>0.01d);
			
			//System.out.println("Final IV="+midPoint+" Calculate Price="+midPointPrice);
			
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(), ex);
		}
		return retVal;
	}

}
