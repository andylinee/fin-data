from financial_data.tasks.worker import app

@app.task()
def crawler(x):
    print("crawler")
    print("upload db")
    return x