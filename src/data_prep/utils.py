
from transformers import BertTokenizer, BertForSequenceClassification, AutoModel
from transformers import pipeline
import numpy as np
import torch

model = BertForSequenceClassification.from_pretrained("D:\models\FinancialBERT-Sentiment-Analysis")
tokenizer = BertTokenizer.from_pretrained("D:\models\FinancialBERT-Sentiment-Analysis")

nlp = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

def comp_sent(texts):
    sent_res = []
    for text in texts:
        res = nlp(text)

        # Extract the first result (assuming single text processing)
        sentiment = res[0]
        sent_res.append([float(sentiment['label'] == 'positive') * sentiment['score'], float(sentiment['label'] == 'negative') * sentiment['score']])
    res = list(np.mean(sent_res, axis=0)) if len(sent_res) > 0 else [0, 0]
        
    # Create a dictionary with sentiment columns
    return {
        'positive': res[0],
        'negative': res[1]
    }
    
def comp_emb(tweet_list):
    # Concatenate tweets with special tokens
    text = ' [SEP] '.join(tweet_list)
    text = '[CLS] ' + text
    
    # Tokenize the text
    inputs = tokenizer(
        text, 
        padding=True, 
        truncation=True, 
        return_tensors="pt"
    )
    
    # Get embeddings using the model
    with torch.no_grad():
        outputs = model(**inputs, output_hidden_states=True)
        # Use the last hidden state of the [CLS] token
        embeddings = outputs.hidden_states[-1][:, 0, :].reshape(-1).tolist()  
    
    return embeddings