package com.nexcorio.algo.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class TelegramUtil {

	private static final Logger log = LogManager.getLogger(TelegramUtil.class);
	
	public static Long postTelegramMessage(String aMessage, Long msgId) {
		// Post to Telegram channel - Refer https://sean-bradley.medium.com/get-telegram-chat-id-80b575520659
		URL exchangeLink;
		try {
			exchangeLink = new URL("https://api.telegram.org/" + ApplicationConfig.getProperty("telegram.channel.id")+ "@" + (msgId!=null?"&reply_to_message_id="+msgId:"") + "&text="     + URLEncoder.encode(aMessage, "UTF-8") ); // This is for GROUP "Nexcorio-FnO Algo"
			
			URLConnection urlConnection = exchangeLink.openConnection();
			urlConnection.setConnectTimeout(5000);
			BufferedReader in = new BufferedReader(	new InputStreamReader(urlConnection.getInputStream()));
			String inputLine;			
			while ((inputLine = in.readLine()) != null){
				log.info(inputLine);
				
				JSONObject resultNode = (JSONObject) ((JSONObject) (new JSONParser()).parse(inputLine)).get("result");
				msgId = (Long) resultNode.get("message_id");
				log.info("~"+msgId);
			}
			in.close();
		} catch (Exception e) {
			log.error("Error In postTelegramMessage "+e.getMessage(), e);
			e.printStackTrace();
		}
		return msgId;
	}
}
