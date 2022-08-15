class Oop1:
    def __init__(self):
        self.name = "Rama Krishna"
        self.age = 32
        self.sal = 40000

    def empdetails(self):
        print("employee {0} age is {1} and his salary is {2}".format(self.name, self.age, self.sal))

class constr:
    def __init__(self, name, age, sal, comm):
        self.name = name
        self.age = age
        self.sal = sal
        self.comm = comm

    def total_sal(self, salary, commission):
        return print("Total salary is :" + str((salary + commission)))


if __name__ == '__main__':
    emp1 = Oop1()
    emp1.empdetails()

    saldetails = constr("Rama Krishna", 32, 4000, 500)
    saldetails.total_sal(4500, 500)
    # print(Oop1())

