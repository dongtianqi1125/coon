package cn.ms.coon.support.common;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.neural.NURL;

public class MregCommon {
	
	private static final Logger logger = LoggerFactory.getLogger(MregCommon.class);
	
	public static final String LOCALHOST = "127.0.0.1";
    public static final String ANYHOST = "0.0.0.0";
    
    private static volatile InetAddress LOCAL_ADDRESS = null;
    private static final Map<String, String> hostNameCache = new HashMap<String, String>(1000);
    private static final Pattern IP_PATTERN = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3,5}$");
	private static final Pattern KVP_PATTERN = Pattern.compile("([_.a-zA-Z0-9][-_.a-zA-Z0-9]*)[=](.*)"); // key value pair pattern.

	public static boolean isEmpty2(String value) {
		return value == null || value.length() == 0 
    			|| "false".equalsIgnoreCase(value) 
    			|| "0".equalsIgnoreCase(value) 
    			|| "null".equalsIgnoreCase(value) 
    			|| "N/A".equalsIgnoreCase(value);
	}
	
	public static boolean isEmpty(String str) {
		if (str == null || str.length() == 0) {
			return true;
		}
		
		return false;
	}

	public static boolean isEquals(String s1, String s2) {
		if (s1 == null && s2 == null)
			return true;
		if (s1 == null || s2 == null)
			return false;
		
		return s1.equals(s2);
	}

	public static boolean isContains(String values, String value) {
		if (values == null || values.length() == 0) {
			return false;
		}
		
		return isContains(Consts.COMMA_SPLIT_PATTERN.split(values), value);
	}

	public static boolean isContains(String[] values, String value) {
		if (value != null && value.length() > 0 && values != null
				&& values.length > 0) {
			for (String v : values) {
				if (value.equals(v)) {
					return true;
				}
			}
		}
		
		return false;
	}

	public static Map<String, String> parseQueryString(String qs) {
		if (qs == null || qs.length() == 0) {
			return new HashMap<String, String>();
		}
		
		return parseKeyValuePair(qs, "\\&");
	}
	
	public static boolean isMatchCategory(String category, String categories) {
        if (categories == null || categories.length() == 0) {
            return Consts.DEFAULT_CATEGORY.equals(category);
        } else if (categories.contains(Consts.ANY_VALUE)) {
            return true;
        } else if (categories.contains(Consts.REMOVE_VALUE_PREFIX)) {
            return ! categories.contains(Consts.REMOVE_VALUE_PREFIX + category);
        } else {
            return categories.contains(category);
        }
    }

    public static boolean isMatch(NURL consumerUrl, NURL providerUrl) {
        String consumerInterface = consumerUrl.getServiceInterface();
        String providerInterface = providerUrl.getServiceInterface();
        if( ! (Consts.ANY_VALUE.equals(consumerInterface) || isEquals(consumerInterface, providerInterface)) ) return false;
        
        if (! isMatchCategory(providerUrl.getParameter(Consts.CATEGORY_KEY, Consts.DEFAULT_CATEGORY), 
                consumerUrl.getParameter(Consts.CATEGORY_KEY, Consts.DEFAULT_CATEGORY))) {
            return false;
        }
        if (! providerUrl.getParameter(Consts.ENABLED_KEY, true) && ! Consts.ANY_VALUE.equals(consumerUrl.getParameter(Consts.ENABLED_KEY))) {
            return false;
        }
       
        String consumerGroup = consumerUrl.getParameter(Consts.GROUP_KEY);
        String consumerVersion = consumerUrl.getParameter(Consts.VERSION_KEY);
        String consumerClassifier = consumerUrl.getParameter(Consts.CLASSIFIER_KEY, Consts.ANY_VALUE);
        
        String providerGroup = providerUrl.getParameter(Consts.GROUP_KEY);
        String providerVersion = providerUrl.getParameter(Consts.VERSION_KEY);
        String providerClassifier = providerUrl.getParameter(Consts.CLASSIFIER_KEY, Consts.ANY_VALUE);
        return (Consts.ANY_VALUE.equals(consumerGroup) || isEquals(consumerGroup, providerGroup) || isContains(consumerGroup, providerGroup))
               && (Consts.ANY_VALUE.equals(consumerVersion) || isEquals(consumerVersion, providerVersion))
               && (consumerClassifier == null || Consts.ANY_VALUE.equals(consumerClassifier) || isEquals(consumerClassifier, providerClassifier));
    }
    
    public static String getLocalHost(){
        InetAddress address = getLocalAddress();
        return address == null ? LOCALHOST : address.getHostAddress();
    }
    
    /**
     * 遍历本地网卡，返回第一个合理的IP。
     * 
     * @return 本地网卡IP
     */
    public static InetAddress getLocalAddress() {
        if (LOCAL_ADDRESS != null) {
        	return LOCAL_ADDRESS;
        }
        
        InetAddress localAddress = getLocalAddress0();
        LOCAL_ADDRESS = localAddress;
        return localAddress;
    }
    
    public static String getHostName(String address) {
    	try {
    		int i = address.indexOf(':');
    		if (i > -1) {
    			address = address.substring(0, i);
    		}
    		String hostname = hostNameCache.get(address);
    		if (hostname != null && hostname.length() > 0) {
    			return hostname;
    		}
    		InetAddress inetAddress = InetAddress.getByName(address);
    		if (inetAddress != null) {
    			hostname = inetAddress.getHostName();
    			hostNameCache.put(address, hostname);
    			return hostname;
    		}
		} catch (Throwable e) {
			// ignore
		}
    	
		return address;
    }
    
    private static Map<String, String> parseKeyValuePair(String str, String itemSeparator) {
		String[] tmp = str.split(itemSeparator);
		Map<String, String> map = new HashMap<String, String>(tmp.length);
		for (int i = 0; i < tmp.length; i++) {
			Matcher matcher = KVP_PATTERN.matcher(tmp[i]);
			if (matcher.matches() == false)
				continue;
			map.put(matcher.group(1), matcher.group(2));
		}
		
		return map;
	}
    
    private static boolean isValidAddress(InetAddress address) {
        if (address == null || address.isLoopbackAddress()) {
        	return false;
        }
        
        String name = address.getHostAddress();
        return (name != null && ! ANYHOST.equals(name) && ! LOCALHOST.equals(name)  && IP_PATTERN.matcher(name).matches());
    }
    
    private static InetAddress getLocalAddress0() {
        InetAddress localAddress = null;
        
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    try {
                        NetworkInterface network = interfaces.nextElement();
                        Enumeration<InetAddress> addresses = network.getInetAddresses();
                        if (addresses != null) {
                            while (addresses.hasMoreElements()) {
                                try {
                                    InetAddress address = addresses.nextElement();
                                    if (isValidAddress(address)) {
                                        return address;
                                    }
                                } catch (Throwable e) {
                                    logger.warn("Failed to retriving ip address, " + e.getMessage(), e);
                                }
                            }
                        }
                    } catch (Throwable e) {
                        logger.warn("Failed to retriving ip address, " + e.getMessage(), e);
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn("Failed to retriving ip address, " + e.getMessage(), e);
        }
        
        if (localAddress == null) {
        	try {
                localAddress = InetAddress.getLocalHost();
                if (isValidAddress(localAddress)) {
                    return localAddress;
                }
            } catch (Throwable e) {
                logger.warn("Failed to retriving ip address, " + e.getMessage(), e);
            }
        }
        
        logger.error("Could not get local host ip address, will use 127.0.0.1 instead.");
        return localAddress;
    }
    
}
