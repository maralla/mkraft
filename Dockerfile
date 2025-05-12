# Use a minimal Alpine image as the base
# Stage 1: Build the application
FROM golang:1.24-alpine as builder

# Install build dependencies
RUN apk add --no-cache make gcc musl-dev

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . .

# Run the build phase
RUN make build

# Stage 2: Create a minimal runtime image
FROM alpine:latest

# Set the working directory in the container
WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/bin/mkraft /app/
COPY --from=builder /app/config /app/

# Define the default command (update as needed)
CMD ["./mkraft"]
