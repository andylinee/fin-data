from tasks import crawler

# Two ways to send tasks
# 1.
crawler.delay(x=0)
# 2.
# task = crawler.s(x=0)
# task.apply_async()
