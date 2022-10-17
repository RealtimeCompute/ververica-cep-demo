package com.alibaba.ververica.cep.demo.condition;

import com.alibaba.ververica.cep.demo.event.Event;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;
import org.apache.flink.cep.dynamic.condition.CustomArgCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class CustomMiddleCondition extends CustomArgCondition<Event> {

    private static final long serialVersionUID = 1L;
    private final String eventArgsSchema;

    private final transient Expression compiledExpression;

    public CustomMiddleCondition(String args, String className) {
        super(args, className);
        String[] argsArr = args.split(";");
        if (argsArr.length < 1) {
            throw new IllegalArgumentException("No args found.");
        }
        compiledExpression = AviatorEvaluator.compile(argsArr[0], false);
        eventArgsSchema = argsArr[1];
    }

//    public CustomMiddleCondition(String expression, @Nullable String filterField) {
//        this.expression =
//                StringUtils.isNullOrWhitespaceOnly(filterField)
//                        ? requireNonNull(expression)
//                        : filterField + requireNonNull(expression);
//        checkExpression(this.expression);
//        compiledExpression = AviatorEvaluator.compile(expression, false);
//    }

    private void checkExpression(String expression) {
        try {
            AviatorEvaluator.validate(expression);
        } catch (ExpressionSyntaxErrorException | CompileExpressionErrorException e) {
            throw new IllegalArgumentException(
                    "The expression of AviatorCondition is invalid: " + e.getMessage());
        }
    }

    public Object getVariableValue(Event propertyBean, String variableName)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = propertyBean.getClass().getDeclaredField(variableName);
        field.setAccessible(true);
        return field.get(propertyBean);
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
                JsonParser parser = factory.createParser((String)variableValue);
                JsonNode actualObj = mapper.readTree(parser);
                variables.put(variableName, actualObj);
            }
            if (!Objects.isNull(variableValue)) {
                variables.put(variableName, variableValue);
            }
        }

        return (Boolean) compiledExpression.execute(variables);
    }
}
