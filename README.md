# UN1
UNMASQUE code for https://dsl.cds.iisc.ac.in/publications/report/TR/TR-2021-02_updated.pdf  


Extraction Features:
____________________
Single Query Extraction  
From Clause  
Correlated Sampling  
View Minimization  
Equi-Join Extraction  
Filter Predicates Extraction  
Projection Extraction  
Group By Extraction  
Aggregation Extraction (on Single Attribute)  
Group By Extraction  
Order By Extraction  
Limit Extraction  

  
Code structure:  
_______________
UN1  
|--mysite  
&nbsp;&nbsp;&nbsp;  |--unmasque  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;    |---refactored &nbsp;&nbsp;&nbsp;    # refactored from the codebase developed in various thesis  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;    |---src &nbsp;&nbsp;&nbsp;&nbsp;           # newly written logic, often to simplify existing code  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;    |---test &nbsp;&nbsp;&nbsp;&nbsp;          # unit testcases for each extractor module  
