from utilites import coordinate_mapping
import sys,os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

if __name__ == "__main__":

    try:
        api_dataframe = coordinate_mapping.df_map_base_url_with_coordinates()

        print(api_dataframe)

        cleaned_dataframe = coordinate_mapping.data_cleansing(dataframe=api_dataframe)

        print(cleaned_dataframe)

        coordinate_mapping.write_to_postgres_db(dataframe=cleaned_dataframe)

        coordinate_mapping.write_to_local(dataframe=cleaned_dataframe)

    except Exception as e:
        print("An error occurred:", str(e))

    finally:
        print("Job completed...")

