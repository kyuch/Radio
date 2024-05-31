# This is a sample Python script.
import telnetlib
host = "cluster.n2wq.com"
port = 7373
login = "LZ3NY"
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def run():
    tn = telnetlib.Telnet(host, port)
    tn.read_until(b'login: ')
    tn.write(login.encode() + b'\n')
    tn.write(b'SET/SKIMMER\n')
    tn.write(b'SET/NOCW\n')
    tn.write(b'SET/NOFT4\n')
    tn.write(b'SET/NORTTY\n')


    for n in range(150):
        data = tn.read_until(b'\n').decode()
        print(data)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
