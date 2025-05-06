import datetime
import pandas as pd

from typing import Any, Dict, List, Union

class Utils:
    @classmethod
    def concat_dataframes(
        cls, dest: pd.DataFrame(), source: pd.DataFrame(), reset_index=False
    ) -> pd.DataFrame():
        """
        The function `concat_dataframes` concatenates two DataFrames, optionally resetting the index.
        
        :param cls: The `cls` parameter in the `concat_dataframes` function is typically used as a
        reference to the class itself. However, in this function, it is not being used. It seems like it
        was mistakenly included in the function definition. If you don't need it, you can remove it from
        :param dest: The `dest` parameter in the `concat_dataframes` function is expected to be a pandas
        DataFrame where the data from the `source` DataFrame will be concatenated. If the `dest`
        DataFrame is empty (has zero rows), the function will simply return the `source` DataFrame as
        the result
        :type dest: pd.DataFrame()
        :param source: The `source` parameter in the `concat_dataframes` function refers to a pandas
        DataFrame that contains the data you want to concatenate with the `dest` DataFrame. It is the
        DataFrame that you want to append or concatenate to the `dest` DataFrame
        :type source: pd.DataFrame()
        :param reset_index: The `reset_index` parameter in the `concat_dataframes` function is a boolean
        flag that determines whether the index of the resulting concatenated DataFrame should be reset
        after the concatenation operation. If `reset_index` is set to `True`, the index of the resulting
        DataFrame will be reset. If it, defaults to False (optional)
        :return: The function `concat_dataframes` returns a pandas DataFrame after concatenating the
        `dest` and `source` DataFrames along the rows (axis=0) and optionally resetting the index if the
        `reset_index` parameter is set to True.
        """
        if len(dest) == 0:
            res = source
        else:
            res = pd.concat([dest, source], axis=0)
        if reset_index:
            res = res.reset_index()
        return res

    @classmethod
    def concat_dicts(cls, dest: Dict, source: Dict) -> Dict:
        """
        The function `concat_dicts` takes two dictionaries `dest` and `source` and concatenates them
        using the `|` operator.
        
        :param cls: The `cls` parameter in the `concat_dicts` function appears to be a reference to the
        class itself. However, in the provided implementation, the `cls` parameter is not being used
        within the function. It is common to use `cls` as a reference to the class when defining class
        methods
        :param dest: The `dest` parameter in the `concat_dicts` function is a dictionary where the
        contents of the `source` dictionary will be concatenated or merged into
        :type dest: Dict
        :param source: The `source` parameter in the `concat_dicts` function refers to a dictionary
        containing key-value pairs that you want to add to the `dest` dictionary. When the function is
        called, the key-value pairs from the `source` dictionary will be merged into the `dest`
        dictionary, and the
        :type source: Dict
        :return: a new dictionary that is a result of merging the `source` dictionary into the `dest`
        dictionary using the `|` operator.
        """
        return dest | source

    @classmethod
    def add_audit_info(
        cls,
        dest: Union[Dict, pd.DataFrame],
        updated_at: datetime = None,
        updated_by: str = "",
    ) -> Dict:
        """
        The `add_audit_info` function adds audit information like `updated_at` and `updated_by` to a
        dictionary or a pandas DataFrame.
        
        :param cls: The `cls` parameter in the `add_audit_info` function is a conventional name used to
        represent the class itself. However, in this function, it is not being used within the method.
        If this function is not a static method and needs to access class-level attributes or methods,
        you may want
        :param dest: The `dest` parameter in the `add_audit_info` function is used to specify the
        destination where the audit information will be added. It can be either a dictionary (`Dict`) or
        a pandas DataFrame (`pd.DataFrame`). The function will add the `updated_at` and `updated_by`
        information to
        :type dest: Union[Dict, pd.DataFrame]
        :param updated_at: The `updated_at` parameter in the `add_audit_info` function is a datetime
        object that represents the timestamp when the audit information was last updated. It is a
        required parameter and cannot be None
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `add_audit_info` function is a string that
        represents the user or entity who updated the data. It is used to track and store information
        about who made changes to the data
        :type updated_by: str
        :return: The function `add_audit_info` returns the `dest` parameter after adding the
        `updated_at` and `updated_by` audit information to it. If the `updated_at` parameter is None or
        the `updated_by` parameter is an empty string, exceptions will be raised. If the `dest`
        parameter is not None and is either a dictionary or a pandas DataFrame, the function will add
        the
        """
        if updated_at is None:
            raise Exception("add_audit_info() -> updated_at cannot be None")
        if updated_by is None:
            raise Exception("add_audit_info() -> updated_by cannot be empty")
        if dest is not None:
            if isinstance(dest, dict) or isinstance(dest, pd.DataFrame):
                dest["updated_at"] = updated_at
                dest["updated_by"] = updated_by
            else:
                raise Exception(
                    f"add_audit_info_dicts() -> type:  {type(dest)} is not supported "
                )
        return dest

    @classmethod
    def add_audit_info_nested_dictionaries(
        cls,
        name: str = "",
        dict: Dict[Any, Any] = None,
        updated_at: datetime = None,
        updated_by: str = "system",
    ) -> List[Dict[Any, Any]]:
        """
        The function `add_audit_info_nested_dictionaries` extracts all nested dictionaries that are
        direct values within a given dictionary and adds audit information to them.
        
        :param cls: The `cls` parameter in the `add_audit_info_nested_dictionaries` function represents
        the class itself. It is used within the function to access class methods or attributes
        :param name: The `name` parameter in the `add_audit_info_nested_dictionaries` function is a
        string that represents the name to be added to the nested dictionaries found within the input
        dictionary
        :type name: str
        :param dict: The `dict` parameter in the `add_audit_info_nested_dictionaries` function is
        expected to be a dictionary where nested dictionaries are present as direct values within it.
        The function iterates over the keys of the input dictionary and if a value is itself a
        dictionary, it adds audit information to that nested
        :type dict: Dict[Any, Any]
        :param updated_at: The `updated_at` parameter in the `add_audit_info_nested_dictionaries`
        function is of type `datetime` and represents the timestamp indicating when the data was last
        updated. It is used to track the time of the last update for the nested dictionaries found
        within the input dictionary
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `add_audit_info_nested_dictionaries`
        function is a string parameter with a default value of "system". This parameter is used to
        specify the entity or user who updated the dictionary. If no value is provided for `updated_by`,
        it will default to "system, defaults to system
        :type updated_by: str (optional)
        :return: the updated dictionary with added audit information for nested dictionaries.
        """
        if len(name) == 0:
            raise Exception(
                "add_audit_info_nested_dictionaries() -> name cannot be empty"
            )

        if dict is not None:
            if len(dict) > 0:
                for key in dict:
                    value = dict[key]
                    if isinstance(value, Dict):
                        value["name"] = name
                        dict[key] = cls.add_audit_info(
                            dest=value, updated_at=updated_at, updated_by=updated_by
                        )
        return dict
    
    @classmethod
    def collect_dict_data(cls,
                          name:str="",  
                          res:Dict = None,
                          sub_res:Dict = None,
                          tag:str="",
                          updated_at: datetime = None,
                          updated_by: str = "system"):
        """
        The function `collect_dict_data` collects and processes dictionary data, ensuring the name is
        not empty and handling nested dictionaries.
        
        :param cls: In the provided code snippet, the parameter `cls` is used as a reference to the
        class itself. It is commonly used in class methods in Python to access class attributes and
        methods. In this context, `cls` is likely a reference to the class that contains the
        `collect_dict_data` method
        :param name: The `name` parameter in the `collect_dict_data` function is a string that
        represents the name of the data being collected. It is used as a key or identifier for the data
        being processed within the function
        :type name: str
        :param res: The `res` parameter in the `collect_dict_data` function is used to store the
        collected dictionary data. It is initialized as `None` by default and is expected to be a
        dictionary. This parameter is updated and populated with data during the data collection process
        within the function
        :type res: Dict
        :param sub_res: The `sub_res` parameter in the `collect_dict_data` function is used to pass a
        dictionary or a list of dictionaries that contain additional data to be collected and processed.
        If `sub_res` is a dictionary, it will be added to the `res` dictionary after some modifications.
        If `
        :type sub_res: Dict
        :param tag: The `tag` parameter in the `collect_dict_data` function is used to provide a tag or
        identifier for the specific data collection process. It helps in identifying and tracking the
        progress or status of the data collection operation for a particular set of data
        :type tag: str
        :param updated_at: The `updated_at` parameter in the `collect_dict_data` function is used to
        specify the datetime when the data was last updated. It has a default value of `None`, which
        means if no value is provided when calling the function, it will default to `None`. This
        parameter allows you to
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `collect_dict_data` function is a string
        parameter with a default value of "system". This parameter is used to specify the entity or user
        who updated the data. If no value is provided for this parameter when calling the function, it
        will default to "system, defaults to system
        :type updated_by: str (optional)
        :return: The function `collect_dict_data` returns the dictionary `res` after collecting and
        processing data based on the input parameters and conditions within the function.
        """  
        print(f"{tag}:{name} -> Start data collection")
        if len(name) == 0:
            raise Exception(
                f"{tag}:{name} -> collect_dict_data() -> name cannot be empty"
            )
        if len(sub_res) > 0:
            if not isinstance(sub_res, list) :
                sub_res = [sub_res]
            #if type(sub_res) is Dict:
            for sub_res_item in sub_res:
                if sub_res_item is not None:
                    sub_res_item["name"] = name
                    # nested dics decoration
                    sub_res_item = cls.add_audit_info_nested_dictionaries(
                        name=name,
                        dict=sub_res_item,
                        updated_at=updated_at,
                        updated_by=updated_by,
                    )
                    # concat
                    res = cls.concat_dicts(dest=res, source=sub_res_item)
                else:
                    print(
                        f"{tag}:{name} -> None, i.e. not supported"
                    )
            if len(res) == 0:
                print(f"{tag}:{name} -> No record found")
        print(f"{tag}:{name} -> data collection is complete with SUCCESS")
        return res
    
    def collect_dataframe_data(cls,
                        name:str="",  
                        res:Dict = None,
                        sub_res:Dict = None,
                        tag:str="",
                        updated_at: datetime = None,
                        updated_by: str = "system"): 
        """
        The function `collect_dataframe_data` collects data for a DataFrame with optional sub-data and
        audit information, handling empty name and displaying progress messages.
        
        :param cls: The `cls` parameter in the `collect_dataframe_data` function is typically used as a
        reference to the class itself. It is commonly used within class methods to access class
        attributes or methods. In this context, `cls` seems to be a reference to the class that contains
        the `add_audit_info
        :param name: The `name` parameter in the `collect_dataframe_data` function is a string that
        represents the name of the data being collected. It is a required parameter and cannot be empty
        :type name: str
        :param res: The `res` parameter in the `collect_dataframe_data` function is used to store the
        data collected during the data collection process. It is a dictionary that can be provided as an
        input to the function to store the collected data. If no dictionary is provided, it defaults to
        `None`. The function
        :type res: Dict
        :param sub_res: The `sub_res` parameter in the `collect_dataframe_data` function is a dictionary
        that stores additional data related to the main data being collected. This parameter allows you
        to pass supplementary information that can be added to the main data being processed. If
        `sub_res` is not None, it will be
        :type sub_res: Dict
        :param tag: The `tag` parameter in the `collect_dataframe_data` function is used to provide
        additional information or context for debugging or logging purposes. It helps identify where in
        the codebase a particular message or action is coming from
        :type tag: str
        :param updated_at: The `updated_at` parameter in the `collect_dataframe_data` function is of
        type `datetime` and is used to specify the timestamp when the data was last updated. This
        parameter allows you to track the last update time of the data being collected in the dataframe
        :type updated_at: datetime
        :param updated_by: The `updated_by` parameter in the `collect_dataframe_data` function is a
        string parameter that specifies the entity or system responsible for the update or modification
        of the data. By default, if no value is provided for `updated_by`, it is set to "system". This
        parameter helps in tracking and, defaults to system
        :type updated_by: str (optional)
        """
        print(f"{tag}:{name} -> Start data collection")
        if len(name) == 0:
            raise Exception(
                f"{tag}:{name} -> name cannot be empty"
            )
        if sub_res is not None:
            sub_res["name"] = name
            sub_res = cls.add_audit_info(
                dest=sub_res, updated_at=updated_at, updated_by=updated_by
            )
            res = cls.concat_dataframes(dest=res, source=sub_res)
        else:
            print(
                f"{tag}:{name} ->  None, i.e. not supported for the ticker: {ticker}"
            )
        if len(res) == 0:
            print(f"{tag}:{name} -> No record found for ticker")
        print(f"{tag}:{name} -> data collection is complete with SUCCESS")
        yield res
