
```mermaid
flowchart LR

        
Product --> Product
Carbon-fiber -- "1" --> Product
Steel -- 2 --> Product
Natural-gas -- 100 --> Carbon-fiber
BioChemical -- 10 --> Carbon-fiber
*Co2* -- 25 --> Carbon-fiber
BioChemical(*BioChemical*) -- "1" --> Natural-gas
BioChemical -- 5 --> Steel
```