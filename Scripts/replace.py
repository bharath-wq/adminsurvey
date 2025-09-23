def replace(df):
    import pandas as pd
    import re
    sort(df)
    for x in range(len(df)):
        if type(df['To which other schools did you apply? (If applicable)'][x]) is str: 
            df['To which other schools did you apply? (If applicable)'][x] =  df.loc[x,'To which other schools did you apply? (If applicable)'].replace("\n", ",")
            df['To which other schools did you apply? (If applicable)'][x] =  df.loc[x,'To which other schools did you apply? (If applicable)'].replace(";", ",")
            df['To which other schools did you apply? (If applicable)'][x] =  df.loc[x,'To which other schools did you apply? (If applicable)'].replace(".", "")
            df['To which other schools did you apply? (If applicable)'][x] =  df.loc[x,'To which other schools did you apply? (If applicable)'].replace("/", ",")
            df['To which other schools did you apply? (If applicable)'][x] =  df.loc[x,'To which other schools did you apply? (If applicable)'].replace(" and ", ",")
            df['To which other schools did you apply? (If applicable)'][x] =  df.loc[x,'To which other schools did you apply? (If applicable)'].replace(",,", ",")
            df['To which other schools did you apply? (If applicable)'][x] = re.sub(r"^[^a-zA-Z].*", "", df['To which other schools did you apply? (If applicable)'][x])
    return df

def sort(df):
    for x in range(len(df)):
        if type(df.loc[x, "What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): -"]) is str:
            sort = sorted(df.loc[x, "What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): -"].split(","))
            df.loc[x, "What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): -"] = ",".join(sort)
    for x in range(len(df)):
        if type(df.loc[x, "Program at Krannert to which you applied: -  Selected Choice"]) is str:
            sort = sorted(df.loc[x, "Program at Krannert to which you applied: -  Selected Choice"].split(","))
            df.loc[x, "Program at Krannert to which you applied: -  Selected Choice"] = ",".join(sort)

def get_output_schema():
    import pandas as pd
    return pd.DataFrame({
 			'Survey Year' : prep_string(),
            'Gender' : prep_string(),
            'When did you first begin your graduate business school research with respect to engaging in the application process?' : prep_string(),
 			'Were you a Domestic or International applicant?' : prep_string(),
 			'Did you decide to choose an MBA/MS/Online or other Graduate Program at a different school?' : prep_string(),
            'What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): -' : prep_string(),
             'What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): - Other - Text' : prep_string(),
             'What are your plans for the upcoming year? - Selected Choice' : prep_string(),
             'What are your plans for the upcoming year? - Other - Text' : prep_string(),
             'To which other schools did you apply? (If applicable)' : prep_string(),
             "Please share any additional comments/rationale on your decision to withdraw admittance from Purdue University's Krannert School" : prep_string(),
             'Name of the school that you will be attending:' : prep_string(),
             'What is your race? Select all that apply. - Selected Choice' : prep_string(),
             'Term and year to which you applied:' : prep_string(),
             'What was the modality of the program that you ultimately enrolled in?' : prep_string(),
             'Please select the response that best describes the type of program you enrolled in. Or you can identify the specific program  1' : prep_string(),
             'Program at Krannert to which you applied: -  Selected Choice' : prep_string(),
             'Name of the Program that you will be attending:' : prep_string(),
             'Country' : prep_string(),
             'State' : prep_string(),
             'Recorded Date' : prep_string(),
             'Response ID': prep_string(),
 			})