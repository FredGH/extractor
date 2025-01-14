import pandas as pd

class Utils():
    @classmethod
    def concat_dataframes(cls, dest:pd.DataFrame(), source:pd.DataFrame(), reset_index=True)->pd.DataFrame():
        if len(dest) == 0:
            res = source
        else:
            res = pd.concat([dest, source], axis=0)
        if reset_index:
            res = res.reset_index()
        return res

    @classmethod
    def concat_dicts(cls,dest:{}, source:{})->{}:
        return dest | source 
        