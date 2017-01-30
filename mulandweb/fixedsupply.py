import os
import csv
import math
import shutil
import sys

from muland import Muland


class FixedSupply(object):

    def __init__(self):
        shutil.copy(os.path.join(sys.argv[1], 'input', 'bids_adjustments.csv'), os.path.join(sys.argv[1], 'input', 'bids_adjustments_0.csv'))

    def load_totals(self, demand_path):
        control_totals = [0] * 29
        with open(demand_path, 'r') as csvfile:
            demand_reader = csv.DictReader(csvfile, delimiter=";")
            for row in demand_reader:
                hx = int(row['H_IDX']) - 1
                Hh = float(row['DEMAND'])
                control_totals[hx] = Hh
        return control_totals

    def sum_totals(self, output_path):  # 'output/location.csv'
        loc_totals = [0] * 29
        with open(output_path, 'r') as csvfile:
            loc_reader = csv.reader(csvfile, delimiter=";")
            loc_reader.next()  # throw away the header
            for row in loc_reader:
                hvi = row[2:]
                for hx in range(0, 29):
                    loc_totals[hx] += float(hvi[hx])
        return loc_totals

    def adjust_bids(self, control_totals, loc_totals):
        bid_adjust = [0] * 29
        ctltotal2 = sum(control_totals[:13])
        loctotal2 = sum(loc_totals[:13])
        for hx in range(0, 13):
            bid_adjust[hx] = math.log((control_totals[hx] / ctltotal2) / (loc_totals[hx] / loctotal2))
        ctltotal1 = sum(control_totals[13:])
        loctotal1 = sum(loc_totals[13:])
        for hx in range(13, 29):
            bid_adjust[hx] = math.log((control_totals[hx] / ctltotal1) / (loc_totals[hx] / loctotal1))
        return bid_adjust

    def make_adjustment(self, prior_adjust, bid_adjust):
        outfile = open('temp.csv', 'w')
        with open(prior_adjust, 'r') as infile:  # 'input/bids_adjustments.csv'
            adjust_reader = csv.reader(infile, delimiter=";")
            adjust_writer = csv.writer(outfile, delimiter=";")
            adjust_writer.writerow(adjust_reader.next())  # copy header
            for row in adjust_reader:
                h = int(float(row[0]))
                hx = h - 1
                a0 = float(row[3])  # current adjustment
                a1 = a0 + bid_adjust[hx]
                adjust_writer.writerow(row[:3] + [a1])
        outfile.close()
        shutil.copy('temp.csv', prior_adjust)
        os.remove('temp.csv')
        return

    def initBid_adjustments(self, adjust_path):
        outfile = open('temp.csv', 'w')
        with open(adjust_path, 'r') as infile:
            adjust_reader = csv.reader(infile, delimiter=";")
            adjust_writer = csv.writer(outfile, delimiter=";")
            adjust_writer.writerow(adjust_reader.next())
            for row in adjust_reader:
                adjust_writer.writerow(row[:3] + [0])
        outfile.close()
        shutil.copy('temp.csv', adjust_path)
        os.remove('temp.csv')
        return

    def run(self, modelDir, initBids, mudata):
        muland = Muland(**mudata)
        control_totals = self.load_totals(os.path.join(modelDir, 'input', 'demand.csv'))
        # make backup copy of bid adjustments
        if (initBids):
            self.initBid_adjustments(os.path.join(modelDir, 'input', 'bids_adjustments.csv'))
        iteration = 0
        converged = 0
        net_adjust = [0] * 29
        maxapdiff_1 = 0
        logfile = open('fs_logfile.csv', 'w')
        logwriter = csv.writer(logfile)
        logwriter.writerow(['Iteration', 'RMSE'])
        while (converged == 0):
            maxapdiff = 0
            muland.run()
            loc_totals = self.sum_totals(os.path.join(modelDir, 'output', 'location.csv'))
            if (iteration == 0):
                print 'Iteration 0 complete.'
                loc_totals_1 = loc_totals
            else:
                SSE = 0
                for hx in range(29):
                    SSE += (loc_totals[hx] - loc_totals_1[hx])**2
                    maxapdiff = max(maxapdiff, 100 * abs(control_totals[hx] - loc_totals[hx]) / control_totals[hx])
                MSE = SSE / 29
                RMSE = math.sqrt(MSE)
                if (iteration == 1):
                    maxapdiff_1 = maxapdiff
                logwriter.writerow([iteration, RMSE, maxapdiff])
                print 'Fixed-supply iteration {0}, RMSE = {1}, Max. Abs. Diff = {2}%'.format(iteration, RMSE, maxapdiff)
                if (maxapdiff < 1):
                    converged = 1
                    print 'Fixed-supply run converged, exiting...'
                    continue
                # elif (iteration == 10):
                elif ((iteration == 10) | (maxapdiff > maxapdiff_1)):
                    converged = -1
                    print 'Fixed-supply run not converging, exiting...'
                    continue
                else:
                    converged = 0
                maxapdiff_1 = maxapdiff
            iteration += 1
            bid_adjust = self.adjust_bids(control_totals, loc_totals)
            # shutil.copy(os.path.join(modelDir, 'input', 'bids_adjustments_0.csv'), os.path.join(modelDir, 'input', 'bids_adjustments.csv')) # restore original (initial) bid adjustments prior to any new adjustment
            self.make_adjustment(os.path.join(modelDir, 'input', 'bids_adjustments.csv'), bid_adjust)
            for hx in range(29):
                net_adjust[hx] += bid_adjust[hx]
        logfile.close()
        with open(os.path.join(modelDir, 'output', 'bh.csv'), 'w') as bhFile:
            bhWriter = csv.writer(bhFile, delimiter=';')
            bhWriter.writerow(['Agents', 'Value'])
            for hx in range(29):
                bhWriter.writerow([hx + 1, net_adjust[hx]])
        return converged
