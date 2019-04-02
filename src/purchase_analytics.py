import pandas as pd
import numpy as np
import sys

# obtaining the file names passed through the command line 
pdts = sys.argv[2] #path to input file products.csv that relates products to their departments
ordr = sys.argv[1] #path to input file order_products.csv that contain order information
outf = sys.argv[3] #path to the output file report.csv


# reading the csv file '../input/products.csv' that contains the information about
# which department a product belongs to into a pandas dataframe named product_info
product_info = pd.read_csv(pdts)

# The following statement determines how many departments are there
# We will use this information later to create report file 
num_of_depts = len(product_info['department_id'].value_counts())

# Extracting only the product id and the department id from the imported product_info
# We are going to use this info to merge the department_id information into the
# next dataframe we import which contains the order information
temp = product_info[['product_id','department_id']]

# importing the file '../input/order_products.csv' which contains the order information
# into a pandas data frame order_info 
order_info = pd.read_csv(ordr)

# The following statements merges the department information from products_info into the order_info
product_info_with_dept = order_info.merge(temp, on='product_id', how='left')

# Now we begin to assemble our output into a dataframe which will then be written into
# the output file '../output/report.csv'
# The next 6 lines builds a data frame with columns 'department_id' and 'number_of_orders'
temp = product_info_with_dept.department_id.value_counts(sort=False)
dept_id = temp.index # get the department index
num_of_orders = temp.values # get the values corresponding to the index
ash = np.argsort(dept_id)
otpt = np.concatenate((dept_id[ash], num_of_orders[ash])).reshape(2, len(dept_id)).T # make a data table
report = pd.DataFrame(otpt, pd.Series(otpt[:,0]), pd.Series(['department_id', 'number_of_orders']))

# Following the above approach...
# the next 5 lines builds a data frame with columns 'department_id' and 'number_of_first_orders'
temp = product_info_with_dept[product_info_with_dept.reordered == 0].department_id.value_counts(sort=False)
dept_id = temp.index
num_of_first_orders = temp.values
otpt = np.concatenate((dept_id, num_of_first_orders)).reshape(2, len(dept_id)).T
temp_report = pd.DataFrame(otpt, pd.Series(dept_id), pd.Series(['department_id', 'number_of_first_orders']))

# The next two lines merges the dataframe containing ['department_id', 'number_of_first_orders']
# with dataframe containing ['department_id', 'number_of_orders'] according to the 'department_id'
# This might generate some NA values in the merged dataframe in the 'number_of_first_order' column
# which are then converted to zeros.
report = report.merge(temp_report, on='department_id', how='left')
report = report.fillna(0)

# The next three lines generate a dataframe containing ['department_id', 'percentage']
# This will later be merged with the above dataframe 'report' that now 
# contains ['department_id', 'number_of_orders', 'number_of_first_orders']
pct = np.concatenate((report.department_id, report['number_of_first_orders']/report['number_of_orders'])).reshape(2,len(report.department_id)).T
temp_report = pd.DataFrame(pct, pd.Series(pct[:,0]), pd.Series(['department_id', 'percentage']))
temp_report.department_id = temp_report.department_id.astype(int)

# This line merges the percentage column from the dataframe containing ['department_id', 'percentage']
# with the dataframe containing ['department_id', 'number_of_orders', 'number_of_first_orders']
# based on 'department_id'
report = report.merge(temp_report, on='department_id', how='left')

# Lastly, rounding off the percentage column to 2 decimal places
# and making sure 'number_of_orders' and 'number_of_first_orders' columns are in int format
report.percentage = report.percentage.round(2)
report.number_of_first_orders = report.number_of_first_orders.astype(np.int64)
report.number_of_orders = report.number_of_orders.astype(np.int64)
#pd.options.display.float_format = '{:,.2f}'.format

# The following lines sets the index to department_id and saves the report file in the output directory
report = report.set_index('department_id')
report.to_csv(outf, float_format='%.2f')
