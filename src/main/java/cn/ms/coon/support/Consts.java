package cn.ms.coon.support;

import java.util.regex.Pattern;

/**
 * Constants
 * 
 * @author lry
 */
public class Consts {

	public static final String NODE_KEY= "node";
	public static final String NODE_DEFAULT_VALUE= "default_node";
	public static final String ENV_KEY= "env";
	public static final String ENV_DEFAULT_VALUE= "default_env";
	public static final String APPLICATION_KEY= "application";
	public static final String APPLICATION_DEFAULT_VALUE= "default_app";
	
    public static final String WEIGHT_KEY= "weight";
    public static final int DEFAULT_WEIGHT = 100;
	
	public static final String PROVIDER = "provider";
	public static final String CONSUMER = "consumer";
	
	public static final String REGISTER = "register";
	public static final String UNREGISTER = "unregister";
	public static final String SUBSCRIBE = "subscribe";
	public static final String UNSUBSCRIBE = "unsubscribe";
	
	public static final String CATEGORY_KEY = "category";
	public static final String PROVIDERS_CATEGORY = "providers";
	public static final String CONSUMERS_CATEGORY = "consumers";
	public static final String ROUTERS_CATEGORY = "routers";
	public static final String CONFIGURATORS_CATEGORY = "configurators";
	public static final String DEFAULT_CATEGORY = PROVIDERS_CATEGORY;
	
	public static final String ENABLED_KEY = "enabled";
	public static final String DYNAMIC_KEY = "dynamic";
	public static final int DEFAULT_TIMEOUT = 1000;
	public static final int DEFAULT_REGISTRY_CONNECT_TIMEOUT = 5000;
	
	public static final String REMOVE_VALUE_PREFIX = "-";
	public static final String BACKUP_KEY = "backup";
	public static final String ANYHOST_VALUE = "0.0.0.0";
	public static final String CONNECT_TIMEOUT_KEY = "connect.timeout";
	public static final String TIMEOUT_KEY = "timeout";
	public static final String TRANSPORTER_KEY = "transporter";
	public static final String TRANSPORTER_DEV_VAL = "curator";
	public static final String CHECK_KEY = "check";
	public static final String GROUP_KEY = "group";
	public static final String INTERFACE_KEY = "interface";
	public static final String FILE_KEY = "file";
	public static final String CLASSIFIER_KEY = "classifier";
	public static final String VERSION_KEY = "version";
	public static final String ANY_VALUE = "*";
	public static final Pattern COMMA_SPLIT_PATTERN = Pattern.compile("\\s*[,]+\\s*");
	public final static String PATH_SEPARATOR = "/";
	public static final Pattern REGISTRY_SPLIT_PATTERN = Pattern.compile("\\s*[|;]+\\s*");
	public static final String EMPTY_PROTOCOL = "empty";
	public static final String ADMIN_PROTOCOL = "admin";
	public static final String PROVIDER_PROTOCOL = "provider";
	public static final String CONSUMER_PROTOCOL = "consumer";
	
	/**注册中心是否同步存储文件，默认异步**/
	public static final String REGISTRY_FILESAVE_SYNC_KEY = "save.file";
	/**注册中心失败事件重试事件**/
	public static final String REGISTRY_RETRY_PERIOD_KEY = "retry.period";
	/**重试周期**/
	public static final int DEFAULT_REGISTRY_RETRY_PERIOD = 5 * 1000;
	/**注册中心自动重连时间**/
	public static final String REGISTRY_RECONNECT_PERIOD_KEY = "reconnect.period";
	
	public static final int DEFAULT_REGISTRY_RECONNECT_PERIOD = 3 * 1000;
	public static final String SESSION_TIMEOUT_KEY = "session";
	public static final int DEFAULT_SESSION_TIMEOUT = 60 * 1000;

}
