package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

// workerUploadObject uploads the data in r, creating an object with the given name.
func workerUploadObject(ctx context.Context, r io.Reader, name, contractSet string, minShards, totalShards int) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", fmt.Sprintf("%v/objects/%v", workerAddr, name), r)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth("", workerPass)
	query := req.URL.Query()
	query.Set("contractset", contractSet)
	query.Set("minshards", strconv.Itoa(minShards))
	query.Set("totalshards", strconv.Itoa(totalShards))
	req.URL.RawQuery = query.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload object: %w", err)
	}
	defer io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		err, _ := io.ReadAll(resp.Body)
		return errors.New(string(err))
	}
	return nil
}
