def unisplit(df):
    import pandas as pd        
    df = split_and_expand(df, "To which other schools did you apply? (If applicable)", "other schools split")
    df = split_and_expand(df, "Program at Krannert to which you applied: -  Selected Choice", "kran programs split")
    df = split_and_expand(df, "What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): -", "factors split")
    return df

def split_and_expand(df, col_name, new_col_name):
        df[col_name].fillna("", inplace = True)
        df[new_col_name] = df[col_name].str.split(",")
        df = df.explode(new_col_name)
        return df
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
             'adcom rework': prep_string(),
             'other factors rework': prep_string(),
             'other plans rework': prep_string(),
             'adcom rework pos wordcount': prep_decimal(),
             'adcom rework neg wordcount': prep_decimal(),
             'adcom rework sentence compound': prep_decimal(),
             'adcom rework sentence pos': prep_decimal(),
             'adcom rework sentence neg': prep_decimal(),
             'other factors rework pos wordcount': prep_decimal(),
             'other factors rework neg wordcount': prep_decimal(),
             'other factors rework sentence compound': prep_decimal(),
             'other factors rework sentence pos': prep_decimal(),
             'other factors rework sentence neg': prep_decimal(),
             'other plans rework pos wordcount': prep_decimal(),
             'other plans rework neg wordcount': prep_decimal(),
             'other plans rework sentence compound': prep_decimal(),
             'other plans rework sentence pos': prep_decimal(),
             'other plans rework sentence neg': prep_decimal(),
             'adcom single words': prep_string(),
             'other factors single words': prep_string(),
             'other plans single words': prep_string(),
             "other schools split": prep_string(),
             "factors split": prep_string(),
             "kran programs split": prep_string(),
 			})