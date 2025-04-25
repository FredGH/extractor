import datetime
import pandas as pd
from typing import Dict, Union

class Utils():
    @classmethod
    def concat_dataframes(cls, dest:pd.DataFrame(), source:pd.DataFrame(), reset_index=False)->pd.DataFrame():
        if len(dest) == 0:
            res = source
        else:
            res = pd.concat([dest, source], axis=0)
        if reset_index:
            res = res.reset_index()
        return res

    @classmethod
    def concat_dicts(cls,dest:Dict, source:Dict)->Dict:
        return dest | source 
    
    @property
    def get_utc_now(cls):
        return datetime.datetime.now(datetime.timezone.utc)

    @classmethod
    def add_audit_info(cls,dest:Union[Dict ,pd.DataFrame], updated_at:datetime=None, updated_by:str="system")->Dict:
        if updated_at is None:
            raise Exception("add_audit_info() -> updated_at cannot be None")
        if dest is not None:
            if  isinstance(dest, dict) or isinstance(dest, pd.DataFrame):
                dest["updated_at"] = updated_at
                dest["tz"] = 'UTC'
                dest["updated_by"] = updated_by
            else:   
                raise Exception(f"add_audit_info_dicts() -> type:  {type(res)} is not supported ")
        return dest 
        