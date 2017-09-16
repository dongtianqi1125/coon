package cn.ms.coon.support.mconf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.ms.coon.service.Mconf;
import cn.ms.coon.support.Consts;
import cn.ms.neural.NURL;

import com.alibaba.fastjson.JSON;

public abstract class AbstractMconf implements Mconf {

	private static final Logger logger = LoggerFactory.getLogger(AbstractMconf.class);

	protected NURL nurl;
	protected String ROOT;

	@Override
	public void connect(NURL nurl) {
		this.nurl = nurl;
		this.ROOT = nurl.getParameter(Consts.GROUP_KEY, "mconf");
	}
	@Override
	public NURL getNurl() {
		return nurl;
	}

	@SuppressWarnings("unchecked")
	protected <T> T json2Obj(String json, Class<T> clazz) {
		try {
			if (clazz == null) {
				return (T) JSON.parseObject(json);
			} else {
				return JSON.parseObject(json, clazz);
			}
		} catch (Exception e) {
			logger.error("Serialization exception", e);
			throw e;
		}
	}

	protected String obj2Json(Object obj) {
		return JSON.toJSONString(obj);
	}
	
	public static boolean isNotBlank(CharSequence cs) {
        return !isBlank(cs);
    }

	public static boolean isBlank(CharSequence cs) {
        int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(cs.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }
	
}
