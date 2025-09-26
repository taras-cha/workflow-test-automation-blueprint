from ps_test_blueprint.nyctaxi_functions import *
import sys



def main():
    args = sys.argv
    schema_name = args[1]
    df = get_nyctaxi_trips()
    avg_df = calculate_avg_distance(df)
    avg_df.write.saveAsTable(".".join(["home_karim_hamed", schema_name, "avg_distance"]))

if __name__ == '__main__':
  main()
