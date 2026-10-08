package config

import (
	"errors"
	"fmt"
	"slices"
)

// Add appends ctx. The name must be set and not already taken.
// It does not validate broker fields or save; call Validate and Save afterwards.
func (c *Config) Add(ctx Context) error {
	if ctx.Name == "" {
		return errors.New("context name is required")
	}
	if _, err := c.Find(ctx.Name); err == nil {
		return fmt.Errorf("context %q already exists; pick another name or edit it instead", ctx.Name)
	}
	c.Contexts = append(c.Contexts, ctx)
	return nil
}

// Replace overwrites the context called name with ctx in place, keeping its
// position in the file. ctx may carry a new name; current-context follows a rename.
func (c *Config) Replace(name string, ctx Context) error {
	if ctx.Name == "" {
		return errors.New("context name is required")
	}
	i := c.index(name)
	if i < 0 {
		return c.notFound(name)
	}
	if ctx.Name != name {
		if _, err := c.Find(ctx.Name); err == nil {
			return fmt.Errorf("cannot rename %q to %q: a context with that name already exists", name, ctx.Name)
		}
		if c.CurrentContext == name {
			c.CurrentContext = ctx.Name
		}
	}
	c.Contexts[i] = ctx
	return nil
}

// Remove deletes the context called name. Removing the current context clears
// current-context, so commands then ask for --context instead of using a stale one.
func (c *Config) Remove(name string) error {
	i := c.index(name)
	if i < 0 {
		return c.notFound(name)
	}
	// A fresh slice, so callers holding the old one never see it shift.
	c.Contexts = append(c.Contexts[:i:i], c.Contexts[i+1:]...)
	if c.CurrentContext == name {
		c.CurrentContext = ""
	}
	return nil
}

func (c *Config) index(name string) int {
	for i, ctx := range c.Contexts {
		if ctx.Name == name {
			return i
		}
	}
	return -1
}

// Update applies edit, validates the whole config and saves it to path. On any
// error the config is left exactly as it was, so a rejected edit changes nothing.
func (c *Config) Update(path string, brokers BrokerValidator, edit func(*Config) error) error {
	prevCurrent, prevContexts := c.CurrentContext, slices.Clone(c.Contexts)
	rollback := func() { c.CurrentContext, c.Contexts = prevCurrent, prevContexts }
	if err := edit(c); err != nil {
		rollback()
		return err
	}
	if err := c.Validate(brokers); err != nil {
		rollback()
		return err
	}
	if err := c.Save(path); err != nil {
		rollback()
		return err
	}
	return nil
}
