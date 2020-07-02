package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;

/**
 * Stub listener default implementation
 */
public class WorkflowStatusListenerStub implements WorkflowStatusListener {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowStatusListenerStub.class);

    private RabbitTemplate rabbitTemplate = null;

    private String exchangeName = null;

    private Boolean useMq = null;

    Boolean getUseMq(){
        if(useMq == null){
            SystemPropertiesConfiguration configuration = new SystemPropertiesConfiguration();
            useMq = configuration.getBooleanProperty("USE_RABBITMQ", true);
        }

        return useMq;
    }

    public RabbitTemplate CreateRabbitTemplate(){
        String virtualHost = "/";

        SystemPropertiesConfiguration configuration = new SystemPropertiesConfiguration();
        String uri = configuration.getProperty("RABBITMQ_URI", "amqp://root:123456@rabbitmq.piecloud-infra.svc.cluster.local:5672");
        exchangeName = configuration.getProperty("RABBITMQ_FANOUTEXCHANGE", "fanout_exchange_workflow");

        LOG.debug("create rabbit template...");
        LOG.debug("exchange: " + exchangeName);
        LOG.debug("rabbitmq uri: " + uri);

        URI connectionUri = URI.create(uri);
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(connectionUri);
        connectionFactory.setVirtualHost(virtualHost);

        //����Exchange
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        FanoutExchange exchange = new FanoutExchange(exchangeName);
        admin.declareExchange(exchange);

        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        return rabbitTemplate;
    }

    public void sendMsgToFanout(String msg) {

        try {
            if(rabbitTemplate == null){
                rabbitTemplate = CreateRabbitTemplate();
            }

            rabbitTemplate.convertAndSend(exchangeName, null, msg);
            LOG.debug("convertAndSend: " + exchangeName + "_" + msg);
        } catch (Exception e) {

            if(rabbitTemplate == null){
                LOG.error("create template failed");
            }else{
                LOG.error("converAndSend failed");
            }

            LOG.error(e.toString());
            LOG.error(e.getStackTrace().toString());
        }
    }

    @Override
    public void onWorkflowCompleted(Workflow workflow) {
        if(getUseMq()){
            Map<String, Object> params = new HashMap<>();
            params.put("workflowId", workflow.getWorkflowId());
            params.put("time", workflow.getEndTime());
            params.put("status", "completed");

            String msg = JSON.toJSONString(params);
            sendMsgToFanout(msg);
            LOG.debug(msg);
        }
        LOG.debug("Workflow {} is completed", workflow.getWorkflowId());
    }

    @Override
    public void onWorkflowTerminated(Workflow workflow) {
        if(getUseMq()){
            Map<String, Object> params = new HashMap<>();
            params.put("workflowId", workflow.getWorkflowId());
            params.put("time", workflow.getEndTime());
            params.put("status", "terminated");

            String msg = JSON.toJSONString(params);
            sendMsgToFanout(msg);
            LOG.debug(msg);
        }
        LOG.debug("Workflow {} is terminated", workflow.getWorkflowId());
    }
}
