package cn.ms.coon.support.mreg.exception;

import cn.ms.coon.support.mreg.FailbackMreg;


/**
 * Wrapper异常，用于指示 {@link FailbackMreg}跳过Failback。
 * <p>
 * NOTE: 期望找到其它更常规的指示方式。
 *
 * @author lry
 */
public class SkipFailbackException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SkipFailbackException(Throwable cause) {
        super(cause);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        // do nothing
        return null;
    }
}
