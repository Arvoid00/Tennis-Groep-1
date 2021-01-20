# %% Import statements
import os
import time
import pandas as pd
from sqlalchemy import create_engine
from flask import current_app as app
from tqdm import tqdm
import time
# %% Connect to Oege

#engine = create_engine('mysql+mysqlconnector://username:password@oege.ie.hva.nl/databasename')

def UploadToDB(filename, sep):
  try:
      engine = create_engine(
          'mysql://username:password@domainname.com/databasename')
      print('Connection Established')
  except Exception as e:
      print('Connection Failed', e)

  fp = os.path.join(app.config['UPLOAD_FOLDER'], filename)
  data = pd.read_csv(fp, sep=sep, error_bad_lines=False, index_col=False)
  
  #data.to_sql('DVT', con=engine, if_exists='replace', index=False, chunksize=10000)

  def chunker(seq, size):
      return (seq[pos:pos + size] for pos in range(0, len(seq), size))

  def insert_with_progress(df):
      con = engine.connect()
      chunksize = int(len(df) / 10)
      with tqdm(total=len(df)) as pbar:
          for i, cdf in enumerate(chunker(df, chunksize)):
              replace = "replace" if i == 0 else "append"
              cdf.to_sql(name="datavalley_tennis_new", con=con,
                         if_exists=replace, index=False, chunksize=15000)
              pbar.update(chunksize)
              tqdm._instances.clear()
  
  try:
    #
    #        CLEANING
    #
    
    # Replace spaces with underscores
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()


#     # %% Add Help columns for logging purposes in MySQL

#     df['isActive'] = True
#     df['Updated_at'] = datetime.now()
#     df['Created_at'] = datetime.now()

    # %% Dates to datetime

    df['datum'] = pd.to_datetime(df['datum']).dt.date

    # %% Clean aantal slagen column, removes + icons and sets 0 for empty values

    df['aantal_slagen'] = df['aantal_slagen'].str.replace('+', '')
    df.loc[df['aantal_slagen'].isnull()] = '0'
    df['aantal_slagen'] = df['aantal_slagen'].astype(int)

    # %% Combines names into similar names to reduce spelling errors
    # Lijst hebben met namen hoe het wel moet zijn om te vergelijken, anders pakken we kortste (tijdelijk)


    def match_sequency_stuffers(string):
        names = df['retourneerder'].unique()

        for name in names:
            if name != np.nan:
                continue

            ratio = SequenceMatcher(None, name.lower(), string.lower()).ratio()
            if ratio > 0.85 and ratio != 1.0:
                if (len(name) > len(string)):
                    return string
                else:
                    return name

        return string


    player_names = ['won_game', 'speler_1', 'speler_2', 'retourneerder', 'a4_server_name', 'c1_winner']

    for col_name in player_names:
        df[col_name] = df[col_name].apply(
            lambda name: match_sequency_stuffers(name))
    
    #
    #      CLEANING END
    #
        
    
    #Upload to Database
    insert_with_progress(data)
    print('\nUpload complete')
    
    #Call Stored Procedure
    t0 = time.time()
    function_name = 'P_UpdateRecords'
    
    #parse columns for SQL format
    columns = str(list(data.columns.values))[1:-1].replace("'", "`")
    params = [columns]
    call_stored_procedure_with_params(engine, function_name, params)
    print('Time Elapsed For Procedure In Seconds: ', time.time() - t0)
    
    if os.path.exists(fp):
      os.remove(fp)
    else:
      print("The file does not exist")
  except Exception as e:
    print('Failed', e)
    
def call_stored_procedure_with_params(sql_engine, function_name, params):
    connection = sql_engine.raw_connection()
    try:
        cursor = connection.cursor()
        cursor.callproc(function_name, params)
        cursor.close()
        connection.commit()
        return 'Procedure Executed'
    finally:
        connection.close()
