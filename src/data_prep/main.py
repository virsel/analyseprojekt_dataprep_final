from step1 import main as s1
from step2 import main as s2
from merge import merge
from step3 import main as s3
from step4 import main as s4

stocks = ['MA', 'ORCL', 'INTC', 'KO', 'AAPL', 'AMZN', 'MSFT', 'CSCO', 'GOOG', 'FB']

def main():
    for stock in stocks:
        s1(stock)
        
    merge_name = 'stocks'
    merge(stocks, merge_name)
    
    s2(merge_name)
    s3(merge_name)
    s4(merge_name)
    
    
if __name__ == '__main__':
    main()