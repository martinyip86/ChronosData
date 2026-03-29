graph TD
    %% 数据层 (已完成)
    subgraph Data_Layer [数据与工程层 - 你的强项]
        A1[Binance/OKX WS] -->|ccxt.pro| A2[Redis Stream]
        A2 -->|DBsyncer| A3[(ClickHouse)]
        A4[DailyPatcher/CSV] -.->|审计修复| A3
        A5[GapDetector/Filler] -->|自愈补救| A2
    end

    %% 研发层 (部分完成)
    subgraph Research_Layer [因子研发与预测 - 已有雏形]
        B1[Polars ETL] --> B2[Indicators: OFI/VAMP]
        B2 --> B3[FactorAnalysis: LR Model]
        B3 --> B4{预测信号 Alpha}
    end

    %% 缺失层 (红色部分)
    subgraph Execution_Layer [定价与成交 - 核心缺失]
        C1[Fair Price 定价模型]:::missing
        C2[Inventory Risk 库容控制]:::missing
        C3[Order Manager/OMS]:::missing
        C4[SOR 智能路由]:::missing
    end

    %% 评估层 (需要强化)
    subgraph Evaluation_Layer [评估与回测 - 需要强化]
        D1[In-sample/Out-sample IC] 
        D2[TCA: 滑点/市场冲击分析]:::reinforce
        D3[Backtester: 撮合引擎]:::reinforce
    end

    %% 连线
    B4 --> C1
    C1 --> C3
    C3 -->|API| A1
    A3 --> D3
    C3 -->|Fill Data| D2

    classDef missing fill:#f96,stroke:#333,stroke-dasharray: 5 5;
    classDef reinforce fill:#bbf,stroke:#333,stroke-width:2px;