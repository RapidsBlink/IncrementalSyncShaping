if __name__ == '__main__':
    with open('gc_log.txt') as ifs:
        lines = ifs.readlines()
        lines = map(lambda line: line.strip(), filter(lambda line: 'Allocation Failure' in line, lines))
        sum = 0.0
        for line in lines:
            # print line
            print float(line.split()[7])
            sum += float(line.split()[7])
        print 'sum:', sum
