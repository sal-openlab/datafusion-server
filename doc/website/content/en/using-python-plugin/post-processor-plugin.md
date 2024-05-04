---
title: Post Processor Plugin
weight: 20
---

* Processor module plugin by Python feature
  + Plugin will be triggered after the SQL query
  + Plugin supported [pandas](https://pandas.pydata.org/) for big data analysis
    - Example pivot-table processor plugin included

```json
{
  "sql": "SELECT * FROM Table1, Table2, Table3 WHERE ...",
  "postProcessors": [
    {
      "module": "module1",
      "pluginOptions": {
        "foo": "Options for processor plugin"
      }
    },
    {
      "module": "module2"
    }
  ]
}
```

### Multiple processor module can be chained

{{< mermaid class="optional" >}}
---
title: Processor Module Chain
---
flowchart LR
Table1["RecordBatch 1"]
Table2["RecordBatch 2"]
Table3["RecordBatch n"]

    Query["datafusion-server\nSQL query"]
    
    Table1 & Table2 & Table3 --> Query
    
    Module1["Processor\nModule 1"]
    Module2["Processor\nModule 2"]

    Response["datafusion-serve\nResponder"]

    Query--"RecordBatch"-->Module1

    subgraph pythonPlugin[Python Processor Plugin]
    Module1--"RecordBatch"-->Module2
    end

    Module2--"RecordBatch"-->Response

    classDef rustInstance fill:#08427b,stroke:#0b4884,color:#fff
    classDef pythonInterpreter fill:#1168bd,stroke:#0b4884,color:#fff
    style pythonPlugin fill:none,stroke:#888,stroke-width:2px,stroke-dasharray: 5 5,color:#888
    
    class Table1,Table2,Table3,Query,Response rustInstance
    class Module1,Module2 pythonInterpreter

{{< /mermaid >}}

## TBD
