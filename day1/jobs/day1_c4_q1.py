# Databricks notebook source
word = dbutils.widgets.get('word')

displayed_words = f'Hello {word}'

print(displayed_words)

dbutils.notebook.exit(displayed_words)
