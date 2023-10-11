import pandas as pd
import os
from subprocess import run
import numpy as np

import arcpy

muLandPath = os.environ["MULAND_PATH"] #r'C:\Program Files\mu-land\mu-land.exe'

input_tables = [
    "agents",
    "agents_zones",
    "bids_adjustments",
    "bids_functions",
    "demand",
    "demand_exogenous_cutoff",
    "real_estates_zones",
    "rent_adjustments",
    "rent_functions",
    "subsidies",
    "supply",
    "zones"
]

class landModel:
    def __init__(self):
        pass
    def from_dir(self,prev_run_path):
        # load a mu-land run from a directory
        if not os.path.exists(prev_run_path):
            raise ValueError("Directory does not exist")
        else:
            for table in input_tables:
                csv_path = os.path.join(prev_run_path,"inputs",table+".csv")
                if not os.path.exists(csv_path):
                    raise ValueError("Missing table: "+table+".csv")
                else:
                    try:
                        self.__setattr__(table,pd.read_csv(csv_path, sep=";"))
                    except Exception as e:
                        raise ValueError(f"Error reading table {table}.csv: {e}")
            self.validate()
    def from_gdb(self,file_gdb_path):
        # load mu-Land tables from a geodatabase
        if not os.path.exists(file_gdb_path):
            raise ValueError("Geodatabase does not exist")
        else:
            arcpy.env.workspace = file_gdb_path
            for table in input_tables:
                try:
                    self.__setattr__(table,pd.DataFrame(arcpy.da.FeatureClassToNumPyArray(table, "*")).drop(columns=["OBJECTID"]))
                except Exception as e:
                    raise ValueError(f"Error reading table {table}: {e}")
            self.validate()
    def validate(self):
        print("Validating zonal data...")
        if not isinstance(self.zones,pd.DataFrame):
            raise TypeError("Zonal data is not a Pandas DataFrame")
        self.i_cols = list(self.zones.columns)
        self.nZones = len(self.zones)
        print(f"{self.nZones} zones read.")
        if len(self.zones.iloc[:,0].unique()) != self.nZones:
            raise ValueError("Zone IDs are not unique")
        print("Validating real estate unit data...")
        if not isinstance(self.real_estates_zones,pd.DataFrame):
            raise TypeError("Real estate data is not a Pandas DataFrame")
        if len(self.real_estates_zones.iloc[:,1].unique()) != self.nZones:
            raise ValueError("Zone IDs in real estate data do not match zonal data")
        self.real_estates_zones = self.real_estates_zones.sort_values(by=[self.real_estates_zones.columns[1],
                                                                          self.real_estates_zones.columns[0]]) # real estate types
        self.vi_cols = list(self.real_estates_zones.columns)
        self.nMarket = len(self.real_estates_zones.iloc[:,2].unique())
        self.nTypes = len(self.real_estates_zones.iloc[:,0].unique())
        print(f"{self.nTypes} types of real estate in {self.nMarket} markets detected")
        print("Validating agent data...")
        if not isinstance(self.agents,pd.DataFrame):
            raise TypeError("Agent data not passed as a Pandas DataFrame")
        a_mkt = len(self.agents.iloc[:,1].unique())
        if a_mkt != self.nMarket:
            raise ValueError(f"Inconsistent number of markets in agents ({a_mkt}) and real estate ({self.nMarket}) tables")
        self.h_cols = list(self.agents.columns)
        self.nAgent = len(self.agents)
        print(f"{self.nAgent} agent types read.")
        if not hasattr(self,"bids_functions"):
            raise NameError("Bid functions not loaded")
        if not hasattr(self,"rent_functions"):
            raise NameError("Rent functions not loaded")
        if not self.demand.shape[0] == self.nAgent:
            raise ValueError("Demand table has incorrect number of rows")
        if not self.supply.shape[0] == self.real_estates_zones.shape[0]:
            raise ValueError("Supply table has incorrect number of rows")
        # to-do: add validation for bid and rent adjustments
    def fill_structure(self):
        # fill in structural inputs
        print("Populating structural inputs...")
        h_idx_field = self.agents.columns[0]
        i_idx_field = self.zones.columns[0]
        agent_zone_rows = []
        for i_row in self.zones.itertuples():
            for h_row in self.agents.itertuples():
                agent_zone_row = {}
                agent_zone_row['H_IDX'] = getattr(h_row, h_idx_field)
                agent_zone_row['I_IDX'] = getattr(i_row, i_idx_field)
                agent_zone_row['ACC'] = 0
                agent_zone_row['ATT'] = 0
                agent_zone_rows.append(agent_zone_row)
        self.agents_zones = pd.DataFrame.from_records(agent_zone_rows)
        demand_cutoff_rows = []
        subsidies_rows = []
        v_idx_field = self.real_estates_zones.columns[0]
        i_idx_field = self.real_estates_zones.columns[1]
        m_idx_field = self.real_estates_zones.columns[2]
        for vi_row in self.real_estates_zones.itertuples():
            V_IDX = getattr(vi_row, v_idx_field)
            I_IDX = getattr(vi_row, i_idx_field)
            vi_market = getattr(vi_row, m_idx_field)
            h_mkt_field = self.agents.columns[1]
            for h_row in self.agents.itertuples():
                H_IDX = getattr(h_row, h_idx_field)
                h_market = getattr(h_row, h_mkt_field)
                if (h_market==vi_market):
                    cutoff=1
                else:
                    cutoff=0
                demand_cutoff_rows.append({"H_IDX": H_IDX, "V_IDX": V_IDX, "I_IDX": I_IDX,"DCUTOFF": cutoff})
                subsidies_rows.append({"H_IDX": H_IDX,"V_IDX": V_IDX,"I_IDX": I_IDX,"SUBSIDIES":0}) 
        self.demand_cutoff = pd.DataFrame.from_records(demand_cutoff_rows)
        self.subsidies = pd.DataFrame.from_records(subsidies_rows)
    def evaluate_bids(self,workDir,balance=True,verbose=True,**kwargs):
        if not os.path.exists(workDir):
            print(f"Work directory '{workDir}' does not exist, creating...")
            os.makedirs(workDir)
        if not os.path.exists(os.path.join(workDir,"input")):
            os.makedirs(os.path.join(workDir,"input"))
        # This function evaluates the bid and rent functions using mu-Land
        # This requires that function tables are previously loaded
        if balance==True:
            if verbose==True:
                print("Checking supply/demand balance...")
            # to-do: make agnostic to field names
            re_supply_df = self.real_estates_zones.merge(self.supply, how="inner", on=["V_IDX","I_IDX"])
            re_mkt_totals = re_supply_df.groupby(by="M_IDX")["NREST"].sum()
            ad_demand_df = self.agents.merge(self.demand, how="inner", left_on="IDAGENT", right_on="H_IDX")
            ad_mkt_totals = ad_demand_df.groupby(by="IDMARKET")["DEMAND"].sum()
            mkt_factors = {}
            for m in re_mkt_totals.keys():
                mkt_factors[m] = 1.0
                if ad_mkt_totals[m] != re_mkt_totals[m]:
                    if verbose==True:
                        print(f"Supply and demand are not equal in market {m}")
                    if balance==True:
                        if verbose==True:
                            print("Adjusting supply to match demand...")
                        mkt_factors[m] = ad_mkt_totals[m]/re_mkt_totals[m]
                    else:
                        print("No adjustments made (supply will determine location outputs)!")
            supply_rows = []
            for row in re_supply_df.itertuples():
                supply_row = {}
                supply_row['V_IDX'] = row.V_IDX # to-do: make agnostic to field names
                supply_row['I_IDX'] = row.I_IDX
                supply_row['NREST'] = row.NREST*mkt_factors[row.M_IDX]
                supply_rows.append(supply_row)
            self.supply = pd.DataFrame.from_records(supply_rows)
        if verbose==True:
            print("Populating working directory with input files...")
        # populate working directory with semicolon-delimited CSVs
        self.zones.to_csv(os.path.join(workDir,"input","zones.csv"), sep=';', index=False)
        self.real_estates_zones.to_csv(os.path.join(workDir,"input","real_estates_zones.csv"), sep=';', index=False)
        self.agents.to_csv(os.path.join(workDir,"input","agents.csv"), sep=';', index=False)
        self.agents_zones.to_csv(os.path.join(workDir,"input","agents_zones.csv"), sep=';', index=False)
        self.subsidies.to_csv(os.path.join(workDir,"input","subsidies.csv"), sep=';', index=False)
        self.rent_adjustments.to_csv(os.path.join(workDir,"input","rent_adjustments.csv"), sep=';', index=False)
        self.bids_adjustments.to_csv(os.path.join(workDir,"input","bids_adjustments.csv"), sep=';', index=False)
        self.demand_cutoff.to_csv(os.path.join(workDir,"input","demand_exogenous_cutoff.csv"), sep=';', index=False)
        self.supply.to_csv(os.path.join(workDir,"input","supply.csv"), sep=';', index=False)
        self.demand.to_csv(os.path.join(workDir,"input","demand.csv"), sep=';', index=False)
        self.bids_functions.to_csv(os.path.join(workDir,"input","bids_functions.csv"), sep=';', index=False)
        self.rent_functions.to_csv(os.path.join(workDir,"input","rent_functions.csv"), sep=';', index=False)
        if verbose==True:
            print("Running mu-Land...")
        run([muLandPath, workDir])
        self.location = pd.read_csv(os.path.join(workDir,"output","location.csv"), sep=";", skip_blank_lines=True)
        expected_rows = self.nZones * self.nTypes
        location_rows = self.location.shape[0]
        if location_rows != expected_rows:
            raise ValueError(f"Problem reading location output (rows={location_rows})")
        self.location_probability = pd.read_csv(os.path.join(workDir,"output","location_probability.csv"), sep=";", skip_blank_lines=True)
        location_probability_rows = self.location_probability.shape[0]
        if location_probability_rows != expected_rows:
            raise ValueError(f"Problem reading location probability output (rows={location_probability_rows})")
        self.bids = pd.read_csv(os.path.join(workDir,"output","bids.csv"), sep=";", skip_blank_lines=True)
        bids_rows = self.bids.shape[0]
        if bids_rows != expected_rows:
            raise ValueError(f"Problem reading bids output (rows={bids_rows})")
        self.rents = pd.read_csv(os.path.join(workDir,"output","rents.csv"), sep=";", skip_blank_lines=True)
        rents_rows = self.rents.shape[0]
        if rents_rows != expected_rows:
            raise ValueError(f"Problem reading bids output (rows={rents_rows})")
        self.bh = pd.read_csv(os.path.join(workDir,"output","bh.csv"), sep=";", skip_blank_lines=True)
        bh_rows = self.bh.shape[0]
        if bh_rows != self.nAgent:
            raise ValueError(f"Problem reading bids output (rows={bh_rows})")
    def run_fixedSupply(self,supply_df,demand_df,workDir,balance=1,tolerance=1.0, maxiters=10, minrmse=0.01,**kwargs):
        for k in range(maxiters):
            self.evaluate_bids(supply_df,demand_df,workDir,balance=balance,verbose=False,**kwargs)
            agent_alloc_df = pd.melt(self.location, id_vars=['Realestate','Zone'], var_name='agent_col', value_name='est_loc')
            agent_alloc_df['IDAGENT'] = agent_alloc_df.agent_col.apply(lambda x: int(x.split(']')[0].split('[')[1]))
            alloc_totals = agent_alloc_df.groupby(by="IDAGENT")["est_loc"].sum()
            compare_agents = pd.merge(alloc_totals, self.demand, left_index=True, right_on="H_IDX")
            compare_agents['abs_diff'] = abs(compare_agents.est_loc - compare_agents.DEMAND)
            compare_agents['aux_adj'] = np.log(compare_agents.DEMAND/compare_agents.est_loc)
            new_bh_records = []
            for bh_record in self.bh.itertuples():
                new_bh_record = {}
                new_bh_record['Agents'] = bh_record.Agents
                new_bh_record['Value'] = bh_record.Value + compare_agents.aux_adj[bh_record.Agents-1]
                new_bh_records.append(new_bh_record)
            self.bh = pd.DataFrame.from_records(new_bh_records)
            self.bh.to_csv(os.path.join(workDir,"output","bh.csv"), sep=";", index=False)
            new_adj_records = []
            for adj_record in self.bids_adjustments.itertuples():
                new_adj_record = {}
                new_adj_record['H_IDX'] = adj_record.H_IDX
                new_adj_record['V_IDX'] = adj_record.V_IDX
                new_adj_record['I_IDX'] = adj_record.I_IDX
                new_adj_record['BIDADJ'] = adj_record.BIDADJ + compare_agents.aux_adj[adj_record.H_IDX-1]
                new_adj_records.append(new_adj_record)
            self.bids_adjustments = pd.DataFrame.from_records(new_adj_records)
            maxapdiff = 100*max(compare_agents.abs_diff/compare_agents.DEMAND)
            if (k > 0):
                diff_alloc = pd.merge(alloc_totals, alloc_totals_1, on="IDAGENT")
                diff_alloc['abs_diff'] = abs(diff_alloc.est_loc_x - diff_alloc.est_loc_y)
                RMSE = np.sqrt(np.mean(diff_alloc.abs_diff**2))/np.mean(diff_alloc.est_loc_y)      
                print('Fixed-supply iteration {0}, RMSE = {1}, Max. Abs. Diff = {2}%'.format(k, round(RMSE,2), round(maxapdiff,2)))
                if (maxapdiff < tolerance):
                    print('Fixed-supply run converged, exiting...')
                    break
                elif ((k == maxiters) or (k > 1 and RMSE > RMSE_1) or (RMSE < minrmse)):
                    print('Fixed-supply run not converging, exiting...')
                    break
                RMSE_1 = RMSE # prior RMSE
            else:
                print('Fixed-supply iteration {0}, Max. Abs. Diff = {1}%'.format(k, round(maxapdiff,2)))
            alloc_totals_1 = alloc_totals
