__author__ = "Angelo Leto"
__email__ = "angleto@gmail.com"

import math

def ShannonEntropy(pArray):
    """
    calculate shannon entropy for an array of elements

    :param pArray: array of elements
    :return: the shannon entropy value
    """
    N = float(sum(pArray))
    H = 0
    for i in range(0,len(pArray)):
        try:
            H += ( (float(pArray[i])/N) * math.log((float(pArray[i])/N) + (1 if pArray[i] == 0 else 0)) )
        except:
            H += 0
    return H

def LogLikelihoodRatio(pKTable):
    """
    :param pKTable: table of 2x2 elements stored in a 4 elements vector, with conditional probability
    :return: the log likelihood ratio, see http://tdunning.blogspot.it/2008/03/surprise-and-coincidence.html
    """
    try:
        v = 2 * sum(pKTable) * (ShannonEntropy(pKTable) -
                                ShannonEntropy([pKTable[0] + pKTable[1], pKTable[2] + pKTable[3]]) -
                                ShannonEntropy([pKTable[0] + pKTable[2], pKTable[1] + pKTable[3]]))
    except:
        v = 0
    return v


