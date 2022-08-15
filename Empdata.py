


class Empdata:
    def total_sal(self,dataframe):
        sal = dataframe.sal
        comm = dataframe.comm
        total_sal = sal + comm
        return total_sal

    def rowcount(self,datafrma):
        count = datafrma.count()
        return count

    def aggvals(self,df,grpbycol,sumcol,derivativecol):
        aggdf = df.groupBy(grpbycol).agg(sum(sumcol).alias(derivativecol))
        return aggdf

    def sumsal(self,df):

        return df.sum("sal")

