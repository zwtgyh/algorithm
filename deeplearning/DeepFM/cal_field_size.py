with open ('/home/dc/deeplearning/DeepFM/field_name.txt', 'r') as f:
	column_name_list = eval(f.read())

print(len(column_name_list))
