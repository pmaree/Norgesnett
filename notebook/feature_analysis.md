### Features ranking rules

#### Rule 1: 
*Filter neighborhoods on PV penetration*

The classical definition of PV penetration is Peak PV production divided by Peak Load Apparent power on feeder. 
Here we define penetration as the ratio of plusskunder / neighborhood size.

#### Rule 2
*Filter for neighborhood size*

European-type distribution grids as they have much larger low voltage transformers serving 20 to as many
as 150 residential customers [1.].

#### Rule 3
*Filter for a neighborhood having a minimal number of production points*

This filter is a lower limit on pluss-kunde of a neighborhoods of interest

#### Rule 4
*Filter for a neighborhood having at least 1 production point producing more than limit*

This is a filter on neighborhoods that has at least one AMI producing more than the limit

#### Rule 5
*Filter for neighborhoods that as at least 1 plusskunde net exporting more than limit*

This is a filter on neighborhoods that has at least 1 plusskunder whose net export exceed a certain limit

#### Rule 6
*Filter for neighorhoods with aggregated net export ratio penetration*

This filter considers the percentage of maximum aggregated 24h our neighborhood peak net export to maximum aggregated 24 peak load  


[1.] https://gridarchitecture.pnnl.gov/media/Modern-Distribution-Grid_Volume_II_v2_0.pdf