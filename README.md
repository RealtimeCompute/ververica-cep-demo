# ververica-cep-demo
Demo of Flink CEP with dynamic patterns.

# Usage of CustomArgCondition
在动态CEP支持中，为了提供更丰富的Condition表达能力，我们允许用户通过继承CustomArgCondition示例来自定义Condition描述如何解析事件，并在JSON中通过字符串(String)来描述自定义Condition所需参数，
进而动态构造自定义Condition实例(内测中)。
## CustomArgCondition示例
我们通过CustomMiddleCondition来展示如何编写自定义Condition。
在CustomMiddleCondition中我们希望能这样动态定义表达式及阈值，同时还希望对于事件中的JSON格式的字符串能进行解析，并在表达式中使用这些字段。
例如 事件消息内容如下：
```json
{
  "userId": "u1",
  "eventId": "1",
  "eventName": "car",
  "eventTime": "2022-01-01 11:22:33",
  //用户自定义字段
  "eventArgs": "{\"detail\": {\"price\": 12300}}"
}
```
表达式如下：
```text
eventName == 'car' && eventArgs.detail.price > 10000
```

我们可以借助Aviator进行动态地进行表达式求值，但要递归地解析JSON String的话，我们需要借助Jackson、jsonpath、fastjson等JSON库进行描述。
因此我们可以这样定义自己的`CustomMiddleCondition`：
```java

public class CustomMiddleCondition extends CustomArgCondition<Event> {

    private static final long serialVersionUID = 1L;

    private final transient Expression compiledExpression;

    public CustomMiddleCondition(String args) {
        super(args, CustomMiddleCondition.class.getCanonicalName());
        checkExpression(args);
        // 构造Aviator表达式
        compiledExpression = AviatorEvaluator.compile(args, false);
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
            // 特殊处理eventArgs这一JSON string字段，使用Jackson对其进行解析
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
        // 执行Aviator表达式
        return (Boolean) compiledExpression.execute(variables);
    }

    ...
}
```

对于如下Pattern：
```java
    Pattern<Event, Event> pattern =
            Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                    .where(
                            new CustomMiddleCondition(
                                    "eventArgs.detail.price > 10000"))
                    .followedBy("end")
                    .where(new EndCondition());
```
它对应的在数据库中的JSON描述为：
```json
{
  "name": "end",
  "quantifier": {
    "consumingStrategy": "SKIP_TILL_NEXT",
    "properties": [
      "SINGLE"
    ],
    "times": null,
    "untilCondition": null
  },
  "condition": null,
  "nodes": [
    {
      "name": "end",
      "quantifier": {
        "consumingStrategy": "SKIP_TILL_NEXT",
        "properties": [
          "SINGLE"
        ],
        "times": null,
        "untilCondition": null
      },
      "condition": {
        "className": "com.alibaba.ververica.cep.demo.condition.EndCondition",
        "type": "CLASS"
      },
      "type": "ATOMIC"
    },
    {
      "name": "start",
      "quantifier": {
        "consumingStrategy": "SKIP_TILL_NEXT",
        "properties": [
          "SINGLE"
        ],
        "times": null,
        "untilCondition": null
      },
      "condition": {
        "args": "eventArgs.detail.price > 10000",
        "className": "com.alibaba.ververica.cep.demo.condition.CustomMiddleCondition",
        "type": "CUSTOM_ARGS"
      },
      "type": "ATOMIC"
    }
  ],
  "edges": [
    {
      "source": "start",
      "target": "end",
      "type": "SKIP_TILL_NEXT"
    }
  ],
  "window": null,
  "afterMatchStrategy": {
    "type": "SKIP_PAST_LAST_EVENT",
    "patternName": null
  },
  "type": "COMPOSITE",
  "version": 1
}
```
我们可以在数据库更新该JSON描述的`args`字段，例如修改阈值为eventArgs.detail.price > 20000，CEP作业会相应地进行动态更新。