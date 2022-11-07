# Databricks notebook source
# word = dbutils.widgets.text('word', 'AAA')

# COMMAND ----------

word = dbutils.widgets.get('word')

print_words = f'Hello {word}'

print(print_words)

# COMMAND ----------

dbutils.notebook.exit(print_words)
