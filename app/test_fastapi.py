from fastapi import FastAPI

test_fastapi = FastAPI()

@test_fastapi.get("/")
def root():
    return {"message": "FastAPI with aouuuuuu v2 works!"}
