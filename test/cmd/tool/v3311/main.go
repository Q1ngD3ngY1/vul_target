package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"git.code.oa.com/trpc-go/trpc-go"
	"git.code.oa.com/trpc-go/trpc-naming-polaris/selector"
	"git.woa.com/adp/kb/kb-config/internal/async/scheduler"
	"git.woa.com/adp/kb/kb-config/internal/dao"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	_ "git.code.oa.com/trpc-go/trpc-config-rainbow"
	"git.woa.com/adp/kb/kb-config/internal/config"
)

var (
	cmdService *CmdService
	cmdOnce    sync.Once
	rootCmd    = &cobra.Command{
		Use:     "kb-config-tool",
		Short:   "The command tool for the kb-config application",
		PreRunE: func(cmd *cobra.Command, args []string) error { return initConfig() },
		RunE:    func(cmd *cobra.Command, args []string) error { return cmd.Usage() },
	}

	CorpIDs  []string
	SpaceIDs []string
	AppIDs   []string
	PageSize int
	Debug    bool
)

func init() {
	// More global flags can be added HERE.
	rootFlags := rootCmd.PersistentFlags()
	rootFlags.StringSliceVarP(&CorpIDs, "corp_ids", "c", []string{}, "given corp ids separated by comma")
	rootFlags.StringSliceVarP(&SpaceIDs, "space_ids", "s", []string{}, "space ids separated by comma")
	rootFlags.StringSliceVarP(&AppIDs, "app_ids", "a", []string{}, "given app ids separated by comma")
	rootFlags.IntVarP(&PageSize, "page_size", "p", 10, "page size of list operations")
	rootFlags.BoolVarP(&Debug, "debug", "d", false, "open debug mode, default is false")

	// More sub-commands can be added HERE.
	rootCmd.AddCommand(cmdApp)
	rootCmd.AddCommand(cmdDB)
	rootCmd.AddCommand(cmdDoc)
	rootCmd.AddCommand(cmdQA)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(2)
	} else {
		os.Exit(0)
	}
}

func initConfig() error {
	pwd, err := os.Getwd()
	if err != nil {
		pwd = "./"
	}
	pwd, _ = filepath.Abs(pwd)
	clientConf := filepath.Join(pwd, "conf", "client.yaml")
	if _, err := os.Stat(clientConf); os.IsNotExist(err) {
		clientConf = filepath.Join(filepath.Dir(pwd), "conf", "client.yaml")
	}
	if _, err = os.Stat(clientConf); err != nil {
		return fmt.Errorf("client config file not found: %s", clientConf)
	}
	toolConf := filepath.Join(filepath.Dir(filepath.Dir(clientConf)), "tool.yaml")

	if _, err = os.Stat(toolConf); err != nil {
		// 读取 client.yaml
		confBytes, err := os.ReadFile(clientConf)
		if err != nil {
			return fmt.Errorf("read client config file failed: %w", err)
		}

		// 使用 map 来处理配置，更灵活
		var confObj map[string]interface{}
		if err = yaml.Unmarshal(confBytes, &confObj); err != nil {
			return fmt.Errorf("unmarshal client config file failed: %w", err)
		}

		// 移除 client.filter
		if client, ok := confObj["client"].(map[interface{}]interface{}); ok {
			delete(client, "filter")
		}

		// 尝试读取 trpc_go.yaml 中的 plugins.config 配置
		trpcGoConf := filepath.Join(pwd, "conf", "trpc_go.yaml")
		if _, err := os.Stat(trpcGoConf); err == nil {
			trpcGoBytes, err := os.ReadFile(trpcGoConf)
			if err == nil {
				var trpcGoObj map[string]interface{}
				if err = yaml.Unmarshal(trpcGoBytes, &trpcGoObj); err == nil {
					// 只复制 plugins.config 配置到 tool.yaml
					if plugins, ok := trpcGoObj["plugins"].(map[interface{}]interface{}); ok {
						if pluginConfig, ok := plugins["config"]; ok {
							// 创建 plugins 节点（如果不存在）
							if confObj["plugins"] == nil {
								confObj["plugins"] = make(map[interface{}]interface{})
							}
							// 只设置 config 子节点
							if pluginsMap, ok := confObj["plugins"].(map[interface{}]interface{}); ok {
								pluginsMap["config"] = pluginConfig
							}
						}
					}
				}
			}
		}

		confBytes, err = yaml.Marshal(confObj)
		if err != nil {
			return fmt.Errorf("marshal client config file failed: %w", err)
		}
		if err = os.WriteFile(toolConf, confBytes, 0644); err != nil {
			return fmt.Errorf("write client config file failed: %w", err)
		}
	}

	trpc.ServerConfigPath = toolConf
	selector.RegisterDefault()
	_ = trpc.NewServer()

	// 初始化配置监听，使 config.App() 能够正常工作
	_ = config.Watch()

	return nil
}

func GetCmdService() *CmdService {
	cmdOnce.Do(func() {
		cmdService = newCmdService()
		if err := scheduler.RunTask(cmdService.AdminRdb, dao.NewMySQLClient()); err != nil {
			panic(fmt.Errorf("run task error: %v", err))
		}
	})
	return cmdService
}
