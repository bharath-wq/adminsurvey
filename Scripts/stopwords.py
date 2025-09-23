def stopwords(df):
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    from nltk.stem import WordNetLemmatizer
    nltk.download('punkt')
    nltk.download('stopwords')
    nltk.download('wordnet')
    df['adcom single words'] = ["" for x in range(len(df))]
    df['other factors single words'] = ["" for x in range(len(df))]
    df['other plans single words'] = ["" for x in range(len(df))]
    df["adcom rework"] =["" for x in range(len(df))]
    df["other factors rework"] = ["" for x in range(len(df))]
    df["other plans rework"] =["" for x in range(len(df))]
    for x in range(len(df)):
        if type(df.loc[x, "Please share any additional comments/rationale on your decision to withdraw admittance from Purdue University's Krannert School"]) is str: 
            tokens = word_tokenize(df.loc[x,"Please share any additional comments/rationale on your decision to withdraw admittance from Purdue University's Krannert School"].lower())
            filtered_tokens = [token for token in tokens if token not in stopwords.words('english')]
            wnl = WordNetLemmatizer()
            lemmatized_tokens=[]
            indexList = []
            for word in filtered_tokens:
                token_word = wnl.lemmatize(word)
                lemmatized_tokens.append(token_word)
                index = len(df.index)
                df.loc[index] = df.loc[x]
                df.loc[index, "adcom single words"] = token_word
                indexList.append(index)
            processed_text = ' '.join(lemmatized_tokens)
            df.loc[x, "adcom rework"] = processed_text
            for i in indexList:
                df.loc[i, "adcom rework"] = processed_text
        else:
            continue
  
        if type(df.loc[x, "What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): - Other - Text"]) is str: 
            tokens = word_tokenize(df.loc[x,"What were the deciding factors upon choosing your future MBA/MS Program? (Please Select all that Apply): - Other - Text"].lower())
            filtered_tokens = [token for token in tokens if token not in stopwords.words('english')]
            wnl = WordNetLemmatizer()
            lemmatized_tokens=[]
            for word in filtered_tokens:
                token_word = wnl.lemmatize(word)
                lemmatized_tokens.append(token_word)
                index = len(df.index)
                df.loc[index] = df.loc[x]
                df.loc[index, "other factors single words"] = token_word
                indexList.append(index)
            processed_text = ' '.join(lemmatized_tokens)
            df.loc[x, "other factors rework"] = processed_text
            for i in indexList:
                df.loc[i, "other factors rework"] = processed_text
        else:
            continue
    for x in range(len(df)):
        if type(df.loc[x, "What are your plans for the upcoming year? - Other - Text"]) is str: 
            tokens = word_tokenize(df.loc[x,"What are your plans for the upcoming year? - Other - Text"].lower())
            filtered_tokens = [token for token in tokens if token not in stopwords.words('english')]
            wnl = WordNetLemmatizer()
            lemmatized_tokens=[]
            for word in filtered_tokens:
                token_word = wnl.lemmatize(word)
                lemmatized_tokens.append(token_word)
                index = len(df.index)
                df.loc[index] = df.loc[x]
                df.loc[index, "other plans single words"] = token_word
                indexList.append(index)
            processed_text = ' '.join(lemmatized_tokens)
            df.loc[x, "other plans rework"] = processed_text
            for i in indexList:
                df.loc[i, "other plans rework"] = processed_text
        else:
            continue
    

    sentiment_analysis(df, "adcom rework")
    sentiment_analysis(df, "other factors rework")
    sentiment_analysis(df, "other plans rework")
    return df

def sentiment_analysis(df, colname: str):
    import nltk
    from nltk.sentiment import SentimentIntensityAnalyzer
    sia = SentimentIntensityAnalyzer()
    nltk.download("opinion_lexicon")
    from nltk.corpus import opinion_lexicon
    pos_words = opinion_lexicon.raw("positive-words.txt")[1536:].strip().split("\n")
    neg_words = opinion_lexicon.raw("negative-words.txt")[1540:].strip().split("\n")
    compList = []
    posList = []
    negList= []
    posWordCount = []
    negWordCount = []
    for sentence in df[colname]:
        posCount = 0
        negCount = 0
        if len(sentence) >= 2:
            for word in nltk.word_tokenize(sentence):
                if word in pos_words:
                    posCount+=1
                elif word in neg_words:
                    negCount+=1
            compList.append(sia.polarity_scores(sentence)["compound"])
            posList.append(sia.polarity_scores(sentence)["pos"])
            negList.append(sia.polarity_scores(sentence)["neg"])
            posWordCount.append(posCount)
            negWordCount.append(negCount)
        else:
            compList.append(-2)
            posList.append(-2)
            negList.append(-2)
            posWordCount.append(-2)
            negWordCount.append(-2)
    df[colname + " pos wordcount"] = posWordCount
    df[colname + " neg wordcount"] = negWordCount
    df[colname + " sentence compound"] = compList
    df[colname + " sentence pos"] = posList
    df[colname + " sentence neg"] = negList
            

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
 			})