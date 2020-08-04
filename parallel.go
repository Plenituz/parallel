package lib

import (
	"fmt"
	"github.com/pkg/errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
)

type ParallelResult struct {
	index int
	name string
	Value interface{}
	Error error
}

type NamedFunction struct {
	Func func() (interface{}, error)
	Name string
}

func Parallelize(functions ...func() error) []error {
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(functions))
	messages := make(chan ParallelResult, len(functions))

	for _, function := range functions {
		go func(copy func() error) {
			defer waitGroup.Done()
			e := copy()
			messages <- ParallelResult{ Error: e }
		}(function)
	}
	waitGroup.Wait()
	close(messages)
	results := make([]error, 0, len(functions))
	for result := range messages {
		results = append(results, result.Error)
	}
	return results
}

//similar to ParallelizeWithValue, expect instead of keeping the order of the functions given, you can
//name the functions yourself et lookup the result based on the name.
//you are responsible for making sure that the names you provide are unique.
//if 2 function have the same name, one will override the otherwise, nondeterministically
func ParallelizeWithNamedValue(functions ...NamedFunction) map[string]ParallelResult {
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(functions))
	messages := make(chan ParallelResult, len(functions))

	for _, function := range functions {
		go func(name string, copy func() (interface{}, error)) {
			defer waitGroup.Done()
			v, e := copy()
			messages <- ParallelResult{
				name: name,
				Value: v,
				Error: e,
			}
		}(function.Name, function.Func)
	}
	waitGroup.Wait()
	close(messages)
	results := map[string]ParallelResult{}
	for result := range messages {
		results[result.name] = result
	}

	return results
}

//this function keeps the order of the values returned
//if you give the arguments func1, func2, func3, the return value will be [func1(), func2(), func3()],
// even if the functions didnt execute in this specific order due to the parallelism
func ParallelizeWithValue(functions ...func() (interface{}, error)) []ParallelResult {
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(functions))
	messages := make(chan ParallelResult, len(functions))

	for i, function := range functions {
		go func(index int, copy func() (interface{}, error)) {
			defer waitGroup.Done()
			v, e := copy()
			messages <- ParallelResult{
				index: index,
				Value: v,
				Error: e,
			}
		}(i, function)
	}
	waitGroup.Wait()
	close(messages)
	results := make([]ParallelResult, 0, len(functions))
	for result := range messages {
		results = append(results, result)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].index < results[j].index
	})
	return results
}

//this function keeps the order of the values returned
func ParallelizeInBatch(functions []func() (interface{}, error), batchSize int) ([]interface{}, []error) {
	allValues := make([]interface{}, 0, len(functions))
	errs := make([]error, 0)
	for len(functions) != 0 {
		max := math.Min(float64(len(functions)), float64(batchSize))
		part := functions[:int(max)]
		functions = functions[int(max):]

		fmt.Println("start batch", len(part))
		results := ParallelizeWithValue(part...)
		for _, result := range results {
			if result.Error != nil {
				errs = append(errs, result.Error)
			} else {
				allValues = append(allValues, result.Value)
			}
		}
	}
	return allValues, errs
}

//this function keeps the order of the values returned
//executes `mFunc` `len(payloads)` times in parallel by groups of `batchSize`, with the arguments `payloads[x]...`
//if mFunc returns a non-nil error in any of it's outputs, the execution will be concidered a fail
//if mFunc returns a single value (not including any potential error) then return return value of ParallelizeInBatchPayload
// will be an array of the type of the value returned my mFunc. in this case you can cast it directly (ie: `result.([]MyStruct)`)
//if mFunc returns more than one non-error value, ParallelizeInBatchPayload will return `[]interface{}`. in this case
// you have to cast each item of the array individually.
func ParallelizeInBatchPayload(mFunc interface{}, payloads [][]interface{}, batchSize int) (interface{}, error) {
	mFuncValue := reflect.ValueOf(mFunc)
	if mFuncValue.Kind() != reflect.Func {
		return nil, errors.New("input function is not a function")
	}
	mFuncType := mFuncValue.Type()
	funcArgs := make([]reflect.Type, mFuncType.NumIn())
	for i := range funcArgs {
		funcArgs[i] = mFuncType.In(i)
	}
	funcOuts := make([]reflect.Type, mFuncType.NumOut())
	for i := range funcOuts {
		funcOuts[i] = mFuncType.Out(i)
	}
	errorIndex := -1
	for i, out := range funcOuts {
		if out.AssignableTo(reflect.TypeOf((*error)(nil)).Elem()) {
			errorIndex = i
		}
	}
	singleItemIndex := -1
	if (errorIndex != -1 && len(funcOuts) == 2) || (errorIndex == -1 && len(funcOuts) == 1) {
		singleItemIndex = 0
		if errorIndex == 0 {
			singleItemIndex = 1
		}
	}

	makeFunc := func(payload []interface{}) func() (interface{}, error) {
		return func() (interface{}, error) {
			if len(payload) != len(funcArgs) {
				return nil, errors.New("input argument count doesnt match payload count")
			}
			argsAsValues := make([]reflect.Value, len(payload))
			for i, payloadItem := range payload {
				if !reflect.TypeOf(payloadItem).AssignableTo(funcArgs[i]) {
					return nil, errors.New("payload item is not of a type assignable to argument type")
				}
				argsAsValues[i] = reflect.ValueOf(payloadItem)
			}
			funcResults := mFuncValue.Call(argsAsValues)
			if len(funcResults) == 0 {
				return nil, nil
			}

			//search for an error
			if errorIndex != -1 && !funcResults[errorIndex].IsNil() {
				//found a non nil error
				return nil, funcResults[errorIndex].Interface().(error)
			}

			if singleItemIndex != -1 {
				//there is only one result, returning it as a single value
				return funcResults[singleItemIndex].Interface(), nil
			}
			//there is several return values, returning them as an array
			returnValues := make([]interface{}, 0, len(funcResults))
			for i, funcResult := range funcResults {
				if i == errorIndex {
					continue
				}
				returnValues = append(returnValues, funcResult.Interface())
			}
			return returnValues, nil
		}
	}

	funcs := make([]func() (interface{}, error), len(payloads))
	for i, payload := range payloads {
		funcs[i] = makeFunc(payload)
	}

	results, errs := ParallelizeInBatch(funcs, batchSize)
	if len(errs) != 0 {
		s := make([]string, len(errs))
		for i, e := range errs {
			s[i] = e.Error()
		}
		return nil, errors.New("error on some parallelized execution: " + strings.Join(s, "\n"))
	}
	if len(funcOuts) == 0 {
		return nil, nil
	}
	if singleItemIndex != -1 {
		//cast the returned array to array of the right type
		slice := reflect.MakeSlice(reflect.SliceOf(funcOuts[singleItemIndex]), len(results), len(results))
		for i, result := range results {
			slice.Index(i).Set(reflect.ValueOf(result))
		}
		return slice.Interface(), nil
	}
	return results, nil
}