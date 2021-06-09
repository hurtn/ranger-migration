
from time import perf_counter
from applyPolicies import AclUpdaterService as aclService

async def main(payload:str) -> str:
            
    acentry = ""
    counter = 0
    
    startTimer = perf_counter()

    for sp in payload['SPIDs']:

        for spid in payload['SPIDs'][sp]:

            missingBit = 'user::r--,group::r--,other::r--'

            # TODO: Include defaults
            #  +',default:'+sp+':'+spid + ':'+permissions +missingBit

            acentry = sp+':'+spid + ':'+payload['Permissions'] + ','+missingBit

            counter += await aclService.override_bulk_recursivly(adlPath=payload['AdlPath'], acl=acentry)
            
    stopTimer = perf_counter()

    print(f"{payload['AdlPath']} updated, {counter} processed and completed in {stopTimer-startTimer:.3f}")

    return str(counter)