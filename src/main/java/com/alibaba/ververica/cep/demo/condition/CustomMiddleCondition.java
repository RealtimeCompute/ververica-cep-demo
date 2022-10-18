package com.alibaba.ververica.cep.demo.condition;

import org.apache.flink.cep.dynamic.condition.CustomArgCondition;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.alibaba.ververica.cep.demo.event.Event;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CustomMiddleCondition extends CustomArgCondition<Event> {

    private static final long serialVersionUID = 1L;

    private final transient Expression compiledExpression;

    public CustomMiddleCondition(String args) {
        super(args, CustomMiddleCondition.class.getCanonicalName());
        checkExpression(args);
        compiledExpression = AviatorEvaluator.compile(args, false);
    }

    private void checkExpression(String expression) {
        try {
            AviatorEvaluator.validate(expression);
        } catch (ExpressionSyntaxErrorException | CompileExpressionErrorException e) {
            throw new IllegalArgumentException(
                    "The expression of AviatorCondition is invalid: " + e.getMessage());
        }
    }

    @Override
    public boolean filter(Event event) throws Exception {
        List<String> variableNames = compiledExpression.getVariableNames();
        if (variableNames.isEmpty()) {
            return true;
        }

        Map<String, Object> variables = new HashMap<>();
        for (String variableName : variableNames) {
            Object variableValue = getVariableValue(event, variableName);
            if (variableName.equals("eventArgs")) {
                ObjectMapper mapper = new ObjectMapper();
                JsonFactory factory = mapper.getFactory();
                JsonParser parser = factory.createParser((String) variableValue);
                JsonNode actualObj = mapper.readTree(parser);
                variables.put(
                        variableName,
                        mapper.convertValue(
                                actualObj, new TypeReference<Map<String, Object>>() {}));
            } else {
                if (!Objects.isNull(variableValue)) {
                    variables.put(variableName, variableValue);
                }
            }
        }

        return (Boolean) compiledExpression.execute(variables);
    }

    public Object getVariableValue(Event propertyBean, String variableName)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = propertyBean.getClass().getDeclaredField(variableName);
        field.setAccessible(true);
        return field.get(propertyBean);
    }
}
