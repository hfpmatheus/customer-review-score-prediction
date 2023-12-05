from zenml import pipeline, step

@step
def step_1() -> str:
    """Returns the 'world' string"""
    return "world"

@step(enable_cache=False)
def step_2(input_one: str, input_two: str) -> None:
    combined_str = f"{input_one} {input_two}"
    print(combined_str)

@pipeline
def my_pipeline():
    output_step_one = step_1()
    step_2(input_one="Hello", input_two=output_step_one)

if __name__ == "__main__":
    my_pipeline()