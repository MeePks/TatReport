from datetime import datetime, timedelta

# Get the start date (first day of the previous month)
first_day_of_current_month = datetime.now().replace(day=1)
start_date = (first_day_of_current_month - timedelta(days=1)).replace(day=1)

# Get the end date (last day of the previous month)
end_date = first_day_of_current_month - timedelta(days=1)

# Convert to `date` objects if needed
start_date = start_date.date()
end_date = end_date.date()

print(f"Start Date: {start_date}")
print(f"End Date: {end_date}")
sp_exec_query=f'''
EXEC [dbo].[__getTat] @startDate = Cast('{start_date}' as date), @EndDate = Cast('{end_date} as date)'
'''
print(sp_exec_query)