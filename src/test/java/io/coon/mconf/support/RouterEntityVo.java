package io.coon.mconf.support;

import io.coon.mconf.ApiEntity;
import io.coon.mconf.ConsumerEntity;
import io.coon.mconf.RouterEntity;

public class RouterEntityVo {

	private RouterEntity router;
	private ApiEntity api;
	private ConsumerEntity consumer;

	public RouterEntityVo() {
	}

	public RouterEntityVo(RouterEntity router, ApiEntity api, ConsumerEntity consumer) {
		this.router = router;
		this.api = api;
		this.consumer = consumer;
	}

	public RouterEntity getRouter() {
		return router;
	}

	public void setRouter(RouterEntity router) {
		this.router = router;
	}

	public ApiEntity getApi() {
		return api;
	}

	public void setApi(ApiEntity api) {
		this.api = api;
	}

	public ConsumerEntity getConsumer() {
		return consumer;
	}

	public void setConsumer(ConsumerEntity consumer) {
		this.consumer = consumer;
	}

}
