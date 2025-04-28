import datetime
import pandas as pd

from typing import Dict, Union, List, Any

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
    
    @classmethod
    def add_audit_info(cls,dest:Union[Dict ,pd.DataFrame], updated_at:datetime=None, time_zone:str="", updated_by:str="")->Dict:
        if updated_at is None:
            raise Exception("add_audit_info() -> updated_at cannot be None")
        if len(time_zone) == 0:
            raise Exception("add_audit_info() -> time_zone cannot be empty")
        if len(updated_by) == 0:
            raise Exception("add_audit_info() -> updated_by cannot be empty")
        if dest is not None:
            if  isinstance(dest, dict) or isinstance(dest, pd.DataFrame):
                dest["updated_at"] = updated_at
                dest["tz"] = time_zone
                dest["updated_by"] = updated_by
            else:   
                raise Exception(f"add_audit_info_dicts() -> type:  {type(res)} is not supported ")
        return dest 
    
    @classmethod
    def add_audit_info_nested_dictionaries(cls, name:str="", dict: Dict[Any, Any]=None, updated_at:datetime=None, time_zone:str="", updated_by:str="system") -> List[Dict[Any, Any]]:
        """
        Extracts all nested dictionaries that are direct values within a given dictionary.

        Args:
            data: The input dictionary.

        Returns:
            A list containing all the nested dictionaries found as direct values.
        """
        if len(name) == 0:
            raise Exception("add_audit_info_nested_dictionaries() -> name cannot be empty")
        if dict is None:
            raise Exception("add_audit_info_nested_dictionaries() -> dic cannot be None")
 
        for key in dict:
            value = dict[key]
            if isinstance(value, Dict):
                value["name"] = name
                dict[key] = cls.add_audit_info(dest= value,  updated_at=updated_at, time_zone=time_zone, updated_by=updated_by)
        return dict

        