# ververica-cep-demo
DebugDemo(可以直接本地运行)展示了CEP timeOrMore语法的一个限制，对于如下pattern:
```java
Pattern<Event, ?> pattern =
    Pattern.<Event>begin("debug", AfterMatchSkipStrategy.skipPastLastEvent())
            .where(new DebugCondition())
            .timesOrMore(3)
            .within(Time.milliseconds(1000));
```
给定输入 a1, a2, a3, a4, a5 (假设a类事件都能满足DebugCondition), 输出的结果是a1, a2, a3，而不是a1, a2, a3, a4, a5。
这是因为我们只有"debug"这个pattern，它既是"起始"子pattern，也是"终止"子pattern, 在这种情况下，a1, a2, a3来了之后就满足了全局的终止条件，会直接输出a1, a2, a3作为匹配结果。
又由于我们指定了AfterMatchSkipStrategy.skipPastLastEvent(), a1, a2, a3不会再参与后续新的匹配。

对于如下pattern:
```java
Pattern<Event, ?> pattern =
    Pattern.<Event>begin("debug", AfterMatchSkipStrategy.skipPastLastEvent())
            .where(new DebugCondition())
            .timesOrMore(3)
            .followedBy("end").where(new EndCondition())
            .within(Time.milliseconds(1000));
```

给定输入 a1, a2, a3, a4, a5, e1 (EndCondition), 输出的结果是a1, a2, a3, a4, a5, e1。因为有"end"作为终止子pattern, Flink CEP能按预期地处理"debug"pattern。

因此对于我们的初始需求：一定时间，某事件连续发生至少k次，输出该事件在这段时间内最终发生的总次数(可能大于k)，目前的workaround是添加一个新的终止子pattern，注意EndCondition不能与DebugCondition相同，否则还会触发全局的终止条件，导致提前输出。