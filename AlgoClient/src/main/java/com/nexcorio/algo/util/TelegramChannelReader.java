package com.nexcorio.algo.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.nexcorio.algo.analytics.AnalyticsBaseClass;
import com.nexcorio.algo.util.db.HDataSource;

public class TelegramChannelReader extends AnalyticsBaseClass implements Runnable {

    // Configuration
	private static final String BOT_TOKEN = ApplicationConfig.getProperty("telegram.channel.id"); 
	private static final String CHANNEL_USERNAME = "@NseFnOAutoPicks";  // e.g., @telegram or a channel with @username
    private static final long POLL_INTERVAL_MS = 5000;  // Poll every 5 seconds
    private SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * Tracks the highest update_id we have already processed.
     * Telegram's getUpdates API uses this offset to return only newer messages.
     */
    private volatile long lastUpdateId = -1;

    /**
     * Set of message_ids already printed — guard against duplicates in the same batch.
     */
    private final Set<Long> processedMessageIds = new HashSet<>();

    public TelegramChannelReader() {
		super();
		
		Thread t = new Thread(this, "TelegramListener");
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
    
    @Override
	public void run() {
		// TODO Auto-generated method stub
    	//TelegramChannelReader reader = new TelegramChannelReader();
        System.out.println("   Starting Telegram Channel Reader for: " + CHANNEL_USERNAME);
        System.out.println("   Bot Token:  " + BOT_TOKEN);
        System.out.println("   Polling every: " + POLL_INTERVAL_MS / 1000 + "s\n");
        setLastUpdated();
        do {
        	 try {
        		 Thread.sleep(5000);
        		 //System.out.println("Wokeup");
                 String updates = fetchUpdates();
                 int newMsgCount = processUpdates(updates);

                 if (newMsgCount > 0) {
                     System.out.println("Processed " + newMsgCount + " new message(s).");
                 }
                 if (timeout(15, 29, 0)) {
                	 this.exitReason = " Exiting: Timeout";
                	 this.exitThread = true;
 				}
             } catch (Exception e) {
                 System.err.println("❌ Error fetching updates: " + e.getMessage());
                 e.printStackTrace();
             }
        } while (!this.exitThread);
	}
    
    public static void main(String[] args) {
    	if ("true".equals(ApplicationConfig.getProperty("enable.telegram.integration"))) {
    		TelegramChannelReader TelegramChannelReader= new TelegramChannelReader();
    	} else {
    		System.out.println("Not enabled. Keep it on only on one node");
    	}
    }

    private void setLastUpdated( ) {
    	Connection conn = null;
		try {
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			String fetchNextSeq = "select last_read_message_id from nexcorio_telegram_notification";
			
			ResultSet rs = stmt.executeQuery(fetchNextSeq);
	    	while (rs.next()) {
	    		lastUpdateId  = rs.getLong("last_read_message_id");
			}
			rs.close();
			stmt.close();
			System.out.println("Set last_read_message_id="+lastUpdateId);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
    }
    
    private void saveLastUpdatedId(long lastReadMsgId) {
    	Connection conn = null;
		try {
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			String fetchNextSeq = "UPDATE nexcorio_telegram_notification set last_read_message_id =" + lastReadMsgId;
			
			stmt.executeUpdate(fetchNextSeq);
			stmt.close();
			//System.out.println("Persist last_read_message_id="+lastReadMsgId);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
    }
    
    /**
     * Fetches updates from Telegram's getUpdates API.
     * The 'offset' parameter ensures we only receive messages newer than the last one processed.
     */
    public String fetchUpdates() throws Exception {
        // Build URL with offset to skip already-read updates
        StringBuilder url = new StringBuilder("https://api.telegram.org/bot")
                .append(BOT_TOKEN)
                .append("/getUpdates?offset=");

        if (lastUpdateId > 0) {
            url.append(lastUpdateId + 1);  // next update_id after the last one we processed
        } else {
            url.append("0");  // start from the beginning on first run
        }

        //System.out.println("🔄 Polling: " + url.toString());

        HttpURLConnection connection = createConnection(url.toString());

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }

            return response.toString();
        } finally {
            connection.disconnect();
        }
    }

    /**
     * Processes the JSON response and prints only genuinely new messages.
     * Returns the count of newly printed messages.
     */
    public int processUpdates(String updatesJson) throws Exception {
        // Parse JSON — could return an object {"ok": true, "result": [...]} or a bare array [...]
        if (updatesJson == null || updatesJson.trim().isEmpty()) {
            System.out.println("   (Empty response from Telegram)");
            return 0;
        } 

        String json = updatesJson.trim();
        
        int newCount = 0;
        
        JSONObject jsonRes = ((JSONObject) (new JSONParser()).parse(json));
       
        JSONArray jsonArray = (JSONArray) jsonRes.get("result");
        
        for(int i=0;i<jsonArray.size();i++) {
        	//System.out.println(jsonArray.get(i));
        	JSONObject aObj = (JSONObject)jsonArray.get(i);
        	Long updateId = (Long) aObj.get("update_id");
        	JSONObject channelPost = (JSONObject) aObj.get("channel_post");
        	String message = (String) channelPost.get("text");
        	
        	if (updateId > lastUpdateId) {
        		processNewMessage(message);
        		newCount++;
        		lastUpdateId = updateId;
        		saveLastUpdatedId(lastUpdateId);
        	}
        }

        //System.out.println("   📊 Last processed update_id: " + lastUpdateId);
        return newCount;
    }

    private void processNewMessage(String message) {
    	
    	String[] messageParts = message.split(" ");
    	if (messageParts.length > 0) {
    		if (messageParts[0].trim().equalsIgnoreCase("status") || messageParts[0].trim().equalsIgnoreCase("S")) {
    			sendStrategyStatus(messageParts);
    		} else if (messageParts[0].trim().equalsIgnoreCase("exit") || messageParts[0].trim().equalsIgnoreCase("ex")) {
    			exitStrategy(messageParts);
    		} else {
    			TelegramUtil.postTelegramMessage("@NseFnOAutoPicks", ApplicationConfig.getProperty("zerodha.user.id") + ": " 
    					+ " Invalid command. Available options are\n"
    					+ " Status <algoId> (Or S <algoId>)\n"
    					+ " Exit <algoId>(Or ex <algoId>)\n"
    					+ " If no algoId provided, then it will select all algos where real time orders enabled.");	
    		}
    	}
    }
    
    private void sendStrategyStatus(String[] messageParts) {
    	Connection conn = null;
		try {
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			String fetchNextSeq = "SELECT f_strategy,exit_profit,best_profit,worst_profit,nooforders FROM nexcorio_option_algo_orders_daily_summary WHERE short_date='" + postgresShortDateFormat.format(new Date()) + "' ";
			if (messageParts.length>1) {
				fetchNextSeq = fetchNextSeq + " and f_strategy=" + messageParts[1].trim();
			} else {
				fetchNextSeq = fetchNextSeq + " and f_strategy in (" 
						+ " SELECT id FROM nexcorio_options_algo_strategy WHERE "
						+ " order_enabled_monday = 't' "
						+ " OR order_enabled_tuesday = 't'"
						+ " OR order_enabled_wednesday = 't'"
						+ " OR order_enabled_thursday = 't'"
						+ " OR order_enabled_friday = 't'"
						+ " order by id)";
			}
			
			ResultSet rs = stmt.executeQuery(fetchNextSeq);
	    	while (rs.next()) {
	    		long algoId = rs.getLong("f_strategy");
	    		float exitProfit = rs.getFloat("exit_profit");
	    		TelegramUtil.postTelegramMessage("@NseFnOAutoPicks", ApplicationConfig.getProperty("zerodha.user.id") + "-X" + algoId + ": " 
						+ " Last updated PnL pts = "+CURRENCY_FORMAT.format(exitProfit));
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}	
    }
    
	private void exitStrategy(String[] messageParts) {
		
		Connection conn = null;
		try {
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			String updateSql = "UPDATE nexcorio_options_algo_strategy set manual_exit_enabled='t' WHERE status='Running' ";
			if (messageParts.length>1) {
				updateSql = updateSql + " and id=" + messageParts[1].trim();
			} else {
				updateSql = updateSql + " and id in (" 
						+ " SELECT id FROM nexcorio_options_algo_strategy WHERE "
						+ " order_enabled_monday = 't' "
						+ " OR order_enabled_tuesday = 't'"
						+ " OR order_enabled_wednesday = 't'"
						+ " OR order_enabled_thursday = 't'"
						+ " OR order_enabled_friday = 't'"
						+ " order by id)";
			}
			int recUpdated = stmt.executeUpdate(updateSql);
			stmt.close();
			
			TelegramUtil.postTelegramMessage("@NseFnOAutoPicks", ApplicationConfig.getProperty("zerodha.user.id") + ": " 
					+ recUpdated + " algos marked for manual exit");
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}	
    }
    /**
     * Extracts a text (string) field value from a JSON object.
     */
    private Optional<String> extractTextField(String json, String key) {
        int idx = json.indexOf(key);
        if (idx == -1) return Optional.empty();

        // Skip the colon and whitespace
        String afterKey = json.substring(idx + key.length()).trim();
        if (!afterKey.startsWith("\"")) return Optional.empty();

        StringBuilder result = new StringBuilder();
        boolean escaped = false;
        for (int i = 1; i < afterKey.length(); i++) {
            char c = afterKey.charAt(i);
            if (escaped) {
                // Handle escape sequences like \" or \\
                result.append(c == '"' ? '"' : (c == '\\' ? '\\' : c));
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                break;  // end of string value
            } else {
                result.append(c);
            }
        }

        return Optional.of(result.toString());
    }

    /**
     * Extracts a numeric field from JSON.
     */
    private long extractLong(String json, String key) {
        int idx = json.indexOf(key);
        if (idx == -1) return 0;

        // Skip past the colon and any whitespace
        String afterKey = json.substring(idx + key.length()).trim();

        StringBuilder numStr = new StringBuilder();
        for (char c : afterKey.toCharArray()) {
            if (Character.isDigit(c)) {
                numStr.append(c);
            } else {
                break;  // non-digit stops the number extraction
            }
        }

        try {
            return Long.parseLong(numStr.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Extracts a timestamp from a JSON object.
     */
    private LocalDateTime extractTimestampFromObject(String json) {
        int dateIdx = json.indexOf("\"date\":");
        if (dateIdx == -1) return LocalDateTime.now();

        String afterDate = json.substring(dateIdx + 7);

        StringBuilder ts = new StringBuilder();
        for (char c : afterDate.toCharArray()) {
            if (Character.isDigit(c)) {
                ts.append(c);
            } else {
                break;
            }
        }

        try {
            long unixTime = Long.parseLong(ts.toString());
            return java.time.Instant.ofEpochSecond(unixTime)
                    .atZone(java.time.ZoneId.systemDefault())
                    .toLocalDateTime();
        } catch (NumberFormatException e) {
            return LocalDateTime.now();
        }
    }

    /**
     * Parses a JSON array string into individual object strings.
     */
    private java.util.List<String> parseJsonArray(String jsonStr) throws Exception {
        if (jsonStr.startsWith("[")) {
            int depth = 0;
            StringBuilder currentUpdate = new StringBuilder();
            List<String> updates = new ArrayList<>();

            for (int i = 1; i < jsonStr.length() - 1; i++) {  // skip outer [ and ]
                char c = jsonStr.charAt(i);

                if (c == '[' || c == '{') depth++;
                else if (c == ']' || c == '}') depth--;

                // When we hit a top-level comma, split into separate update objects
                if (depth <= 1 && c == ',') {
                    String trimmed = currentUpdate.toString().trim();
                    if (!trimmed.isEmpty()) updates.add(trimmed);
                    currentUpdate.setLength(0);
                } else {
                    currentUpdate.append(c);
                }
            }

            // Add the last update object
            String trimmed = currentUpdate.toString().trim();
            if (!trimmed.isEmpty()) updates.add(trimmed);

            return updates;
        }

        throw new Exception("Invalid JSON array format");
    }

    /**
     * Creates and configures an HTTP connection to Telegram API.
     */
    private HttpURLConnection createConnection(String urlString) throws Exception {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        // Set headers for better performance
        conn.setRequestProperty("User-Agent", "TelegramChannelReader/1.0");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        conn.setRequestMethod("GET");

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            // Read error body for better diagnostics
            String errorMsg;
            try (BufferedReader errReader = new BufferedReader(
                    new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = errReader.readLine()) != null) {
                    sb.append(line);
                }
                errorMsg = sb.toString();
            } catch (Exception ignored) {
                errorMsg = conn.getResponseMessage();
            }

            throw new Exception("HTTP " + responseCode + ": " + errorMsg);
        }

        return conn;
    }

	
}
