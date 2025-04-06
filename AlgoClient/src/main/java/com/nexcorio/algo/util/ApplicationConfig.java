package com.nexcorio.algo.util;

import java.io.IOException;
import java.util.Properties;


import com.nexcorio.algo.Main;

/**
 * @author Keshav Shetty
 * 
 */
public class ApplicationConfig {
	
	static Properties properties = null;
	
	public static Properties getProperties() {
		if (properties==null) {
			properties = new Properties();
			try {
				properties.load(Main.class.getResourceAsStream("/config.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return properties;
	}
	
	public static String getProperty(String propertyname) {
		if (properties==null) {
			properties = new Properties();
			try {
				properties.load(Main.class.getResourceAsStream("/config.properties"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return properties.getProperty(propertyname);
	}
	
}
	
