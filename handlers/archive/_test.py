from IndexFilterHandler import IndexFilterHandler

query = '''
(
    earliest="2023-05-01" latest="2023-05-31"
    (
        (status="error" OR status="critical") AND errorCode IN (403, 404)
    ) 
    OR 
    (priority="high" AND action="update") 
    index="system_logs/error_tracking"
) OR ( 
    earliest="2023-06-01" latest="2023-06-30" 
    (transactionStatus="pending" AND amount > 1000) 
    AND customerType="VIP" 
    index="finance_logs/transactions"
)| eval note="Example complex query 11."
'''

index_filter_handler = IndexFilterHandler(query)
filtered_data = index_filter_handler.get_filtered_data()
print(filtered_data.head())