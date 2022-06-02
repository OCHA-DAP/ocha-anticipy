import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from random import choice


@dataclass
class QueryParams:
    filepath: Path
    query: dict
    request_id = None


def call_cdsapi_client(name, request):
    print(f"Querying CDS for  {request['year']}...")
    # c = cdsapi.Client(wait_until_complete=False, delete=False)
    # r = c.retrieve(name, request)
    # request_id = r.reply['request_id']  # save this for later
    return f"request_id_{request['year']}"


class Glofas:
    _cds_name = None
    _system_version = None
    _raw_base_dir = Path(".")

    def _get_raw_filepath(self, year):
        return self._raw_base_dir / f"raw_filepath_{year}"

    async def _download_producer(self, queue, query_params):
        query_params.request_id = call_cdsapi_client(
            name=self._cds_name, request=query_params.query
        )
        await queue.put(query_params)
        print("Added to queue")

    @staticmethod
    async def _download_consumer(queue):
        while True:
            query_params = await queue.get()
            state = "init"
            while state != "completed":
                # result = cdsapi.api.Result(new_client, dict(request_id=query_params.request_id))
                # state = result.update().reply['state']
                state = choice(["completed", "in progress"])
                if state == "completed":
                    break
                print(
                    f"For request {query_params.request_id}, state is {state}"
                )
                await asyncio.sleep(2)

            print(f"Request {query_params.request_id} complete")
            # result.download(query_params.filepath)
            queue.task_done()

    def _get_query(self, year):
        return {"system_version": self._system_version, "year": year}


class GlofasReanalysis(Glofas):
    _cds_name = "CDS name"
    _system_version = 3

    def download(self, year_min=1999, year_max=2019, clobber=True):
        asyncio.run(
            self._download_async(
                year_min=year_min, year_max=year_max, clobber=clobber
            )
        )

    async def _download_async(self, year_min, year_max, clobber):
        queue = asyncio.Queue()

        # Create the list of queries
        query_params_list = [
            QueryParams(
                self._get_raw_filepath(year), self._get_query(year=year)
            )
            for year in range(year_min, year_max + 1)
        ]

        producers = [
            asyncio.create_task(self._download_producer(queue, query_params))
            for query_params in query_params_list
        ]
        consumers = [
            asyncio.create_task(self._download_consumer(queue))
            for _ in range(3)
        ]
        await asyncio.gather(*producers)
        await queue.join()  # Implicitly awaits consumers, too
        for c in consumers:
            c.cancel()


if __name__ == "__main__":
    glofas = GlofasReanalysis()
    glofas.download()
