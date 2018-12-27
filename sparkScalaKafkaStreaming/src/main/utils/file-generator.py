import csv

with open('C:\\Users\\singhgo\\Documents\\work\sftp-files\\sample-ftp-file.csv', 'w', newline='') as csvfile:
    fieldnames = ['id', 'username', 'doj']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for i in range(1, 5000000):
        writer.writerow({'id': i, 'username': 'user' + str(i), 'doj': 20181227})
