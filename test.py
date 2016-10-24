import sys
import glob, os
import xlrd
import xlwt
import numpy as np
import re
import csv

def check_flase_value(target):
  #if (target.find(r"\*") > 1 or target.find('\#') > 1 or target.find('x')):
  if (re.match("^[-]*[0-9]*[\*]*[\#]*[x]*$", target)):
    # print(target)
    return 1
  else:
    return 0

segment = 108
target = "total/output_pm25_108_HC.csv"
list_array = []
number = 0

os.chdir("/Users/kenie/Downloads/104_pm2.5")
for file in glob.glob("*.xls"):
  print(file)

  # Read the dataset.xls and the sheet named "Dataset"
  data = xlrd.open_workbook(file)
  table = data.sheet_by_name(u'Sheet1')

  # Get the number of rows in "Sheet1" sheet
  nrows_num = table.nrows

  real_nrows = nrows_num - 1

  # Get the number of rows in "Sheet1" sheet
  nclos_num = table.ncols

  for i in range(nrows_num):
    if (table.cell(i, 2).value == 'PM2.5'):
      # print(table.row_values(i))
      #print(table.cell(i, 2).value)
      for j in range(nclos_num):
        if (j > 2):
          # print(table.cell(i, j).value)
          if (check_flase_value(str(table.cell(i, j).value))):
            list_array.append("")
          else:
            list_array.append(table.cell(i, j).value)
          number += 1
          
# print(list_array)
# print(number)

list_array_bak = list_array

# workbook = xlwt.Workbook()
# sheet = workbook.add_sheet('Sheet1', cell_overwrite_ok = False)

# for i in range(number):
  # sheet.write(i, 2, list_array_bak[i])

for i in range(number):
  if (list_array[i] == ''):
    previous_num = int(list_array[i - 1])
    if (list_array[i + 1] == ''):
      x = i + 2
      for value in range(x, number):
        if (list_array[value] != ''):
          next_num = list_array[value]
          break
        elif (list_array[value] == '' and x == number):
          next_num = previous_num
          break
          # print(value, previous_num)
    else:  
      next_num = int(list_array[i + 1])
    mean = (int(list_array[i - 3]) + int(list_array[i - 2]) + int(list_array[i - 1]) + next_num) / 4
    list_array[i] = mean
    # print(i, previous_num, next_num, mean)

    # sheet.write(i, 1, mean)
  else:
    # sheet.write(i, 1, list_array[i])
    continue

# workbook.save('total/test2.xls')

list_segment = []
for i in range(segment):
  list_segment.append("t" + str(i))

#print(list_segment)

with open(target, 'w', newline='') as out:
  writer = csv.writer(out, delimiter=',')
  writer.writerow(list_segment)

  for y in range(number):
    if (y + segment <= number):
      row_array = list(list_array[slice(y, y + segment)])
      # if (y < 2):
      #   print(row_array)
      writer.writerow(row_array)
    else:
      break

# with open('total/output2.csv', 'w', newline='') as out:
#   writer = csv.writer(out, delimiter=',')
#   writer.writerow(list_segment)

#   for y in range(number):
#     if (y + segment <= number):
#       row_array = list(list_array[slice(y, y + segment)])
#       # if (y < 2):
#       #   print(row_array)
#       writer.writerow(row_array)
#     else:
#       break

# for j in range(nclos_num):
#     print(table.col_values(j))