import anthropic

# 1. Initialize with explicit key or env var
client = anthropic.Anthropic()

# 2. Use updated model and clean syntax
message = client.messages.create(
    model="claude-3-5-sonnet-latest",
    max_tokens=1024,
    messages=[
        {"role": "user", "content": "Hello, Claude"}
    ]
)

# 3. Corrected print statement (syntax fix)
print(message.content[0].text)