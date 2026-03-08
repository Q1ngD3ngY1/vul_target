package linker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLinker_trimPrefix(t *testing.T) {
	linker := New()
	prefix := "prefix: "
	t.Run("len(prefix) > len(content)", func(t *testing.T) {
		content, _ := linker.trimPrefix(context.Background(), prefix, "prefix:")
		assert.EqualValues(t, "prefix:", content)
	})
	t.Run("len(prefix) == len(content)", func(t *testing.T) {
		content, _ := linker.trimPrefix(context.Background(), prefix, "prefix: ")
		assert.EqualValues(t, "", content)
	})
	t.Run("len(prefix) < len(content)", func(t *testing.T) {
		content, _ := linker.trimPrefix(context.Background(), prefix, "prefix: a")
		assert.EqualValues(t, "a", content)
	})
}

func TestLinker_merge(t *testing.T) {
	ctx := context.Background()
	linker := New()
	t.Run("范围未相交, 无法合并", func(t *testing.T) {
		a := Content{Value: "abcdefg", Start: 11, End: 20}
		b := Content{Value: "efghijk", Start: 21, End: 30}
		assert.EqualValues(t, []Content{a, b}, linker.merge(ctx, a, b))
	})
	t.Run("范围相交, 文本无交集, 无法合并", func(t *testing.T) {
		a := Content{Value: "abcdefg", Start: 10, End: 20}
		b := Content{Value: "hijklmn", Start: 15, End: 25}
		assert.EqualValues(t, []Content{a, b}, linker.merge(ctx, a, b))
	})
	t.Run("keep 保持, 不合并1", func(t *testing.T) {
		a := Content{Value: "abcdefg", Start: 0, End: 20, Keep: true}
		b := Content{Value: "efghijk", Start: 10, End: 20}
		assert.EqualValues(t, []Content{a, b}, linker.merge(ctx, a, b))
	})
	t.Run("keep 保持, 不合并2", func(t *testing.T) {
		a := Content{Value: "abcdefg", Start: 0, End: 20}
		b := Content{Value: "efghijk", Start: 10, End: 20, Keep: true}
		assert.EqualValues(t, []Content{a, b}, linker.merge(ctx, a, b))
	})
	t.Run("keep 保持, 不合并3", func(t *testing.T) {
		a := Content{Value: "abcdefg", Start: 0, End: 20, Keep: true}
		b := Content{Value: "efghijk", Start: 10, End: 20, Keep: true}
		assert.EqualValues(t, []Content{a, b}, linker.merge(ctx, a, b))
	})
	t.Run("a 包含 b", func(t *testing.T) {
		a := Content{Value: "abcdefg", Start: 0, End: 20}
		b := Content{Value: "efghijk", Start: 10, End: 20}
		assert.EqualValues(t, []Content{a}, linker.merge(ctx, a, b))
	})
	t.Run("b 包含 a", func(t *testing.T) {
		a := Content{Value: "abcdefg", Start: 10, End: 20}
		b := Content{Value: "efghijk", Start: 0, End: 20}
		assert.EqualValues(t, []Content{b}, linker.merge(ctx, a, b))
	})
	t.Run("a 合并 b", func(t *testing.T) {
		a := Content{Value: "abcdefg", Start: 10, End: 20, idx: 1}
		b := Content{Value: "efghijk", Start: 15, End: 25, idx: 2}
		assert.EqualValues(t, []Content{{Value: "abcdefghijk", Start: 10, End: 25, idx: 1}}, linker.merge(ctx, a, b))
	})
	t.Run("b 合并 a", func(t *testing.T) {
		a := Content{Value: "efghijk", Start: 15, End: 25, idx: 1}
		b := Content{Value: "abcdefg", Start: 10, End: 20, idx: 2}
		assert.EqualValues(t, []Content{{Value: "abcdefghijk", Start: 10, End: 25, idx: 2}}, linker.merge(ctx, a, b))
	})
}

func TestLinker_Merge(t *testing.T) {
	ctx := context.Background()
	linker := New()
	t.Run("范围未相交, 无法合并", func(t *testing.T) {
		a := Content{Prefix: "prefix: ", Key: "k1", Value: "prefix: abcdefg", Start: 10, End: 20}
		b := Content{Prefix: "prefix: ", Key: "k2", Value: "prefix: efghijk", Start: 15, End: 25}
		c := Content{Prefix: "prefix: ", Key: "k1", Value: "prefix: efghijk", Start: 15, End: 25}
		d := Content{Prefix: "prefix: ", Key: "k1", Value: "prefix: klmnopq", Start: 24, End: 27}
		e := Content{Prefix: "prefix: ", Key: "k2", Value: "prefix: fghijkl", Start: 25, End: 26}
		f := Content{Prefix: "prefix: ", Key: "k1", Value: "prefix: opqrst", Start: 25, End: 30, Keep: true}
		g := Content{Prefix: "prefix: ", Key: "k2", Value: "prefix: lmno", Start: 25, End: 30}
		assert.EqualValues(t, []Content{
			{Prefix: "prefix: ", RetrievalPrefix: "prefix: ", Key: "k1", Value: "prefix: abcdefghijklmnopq", Start: 10, End: 27, idx: 0},
			{Prefix: "prefix: ", RetrievalPrefix: "prefix: ", Key: "k2", Value: "prefix: efghijklmno", Start: 15, End: 30, idx: 1},
			{Prefix: "prefix: ", RetrievalPrefix: "prefix: ", Key: "k1", Value: "prefix: opqrst", Start: 25, End: 30, Keep: true, idx: 5},
		}, linker.Merge(ctx, []Content{a, b, c, d, e, f, g}))
	})
	t.Run("范围未相交, 无法合并", func(t *testing.T) {
		a := Content{Key: "1-0", Value: "", Start: 0, End: 0, Prefix: "", Keep: true, idx: 0}
		b := Content{Key: "2-1001", Value: `4.9MB_file: “彼得.亚历山大耶维奇.
罗曼诺夫”司马再一次在口中重复了一遍这个名字，这个俄罗斯名字说起来非常绕口，远不像中国名字容易上口、易记。
“或许？”想着那个拥有蓝色眼睛与金黄又带点红色头发的女孩，司马从她为孩子起的名字中隐约猜出她的本意，她清楚的知道自己永远不可能给她和孩子一切，才会用这种方式希望能够给予孩子一些补偿吧！一个俄罗斯皇室贵族身份，这或许是对他最好的补偿。“还好没有血友病”尽管一时无法接受自己的儿子竟然叫了个外国名，但是看到他的检验报告，一直以来司马最担心的事情并没有发生在他的身上，他并没遗传母亲身上的血友病，这是司马最为欣喜的事情，至少自己的儿子是健康的。“老板，恭喜你！”一直静站在一边的石磊见司马表情复杂，便开口恭喜到。尽管是一个秘密，但是石磊仍然为此感到高兴，毕竟老板有了一个儿子，作为下属自然为他高兴。这件事在西北只有几个人知道而石磊就是其中之一，做为调查部首脑，石磊无论是在伊尔库茨克皇宫或是圣叶卡捷琳堡公主身边，都安插了不少眼线。听到石磊的恭喜，司马哭笑不得叹了口气，和安娜之间是意外，而他同样也是一个意外。`,
			Start:           46898,
			End:             46907,
			Prefix:          "4.9MB_file: ",
			RetrievalPrefix: "4.9MB_file: ",
			Keep:            false,
			idx:             1,
		}
		c := Content{Key: "2-1031", Value: `akSrU45XjTGbyT8xVq0v-7708596796: ”这个理由是司马将近一年来唯一的借口和理由。

“上次你头被人打破，是因为她吧！还没忘记她吗？”父亲当然了解，不到一年前的那次伤害，对自己儿子造成的伤害。但是，为那样的一个女人显然不值。

“爸！我……她早都是过去了，只是现在……我并不适合找老婆，像现在这样几个星期都不在家呆上一天。找个老婆反而麻烦，要不回头在那边我给你找一个儿媳妇吧！如果你们不嫌她年龄大的话。”听着父亲的话，知道父亲是怕自己还没走出感情的阴影，才会对自己有些担心，于是便半开玩笑的说到。

“行啊！只要你不觉得老，自己个愿意就行，那个时代的儿媳妇可都比现代的要贤慧很多。记着别忘了把我孙子给我们送过来就行。”父亲一听也乐了。自己儿子要在那个时候找媳妇，那生日可都是上个世纪的。

当晚，司马在家里陪父母一起在家吃个团圆饭，很久都没喝酒的父亲也喝了数杯，这一晚父子两人都醉了，谁知道下一次司马再次回来，一家人在一起吃团圆饭是什么时候？
   当司马离开家的时候，父亲什么都没说，和母亲在客厅里看着电视，临行时看着有些空荡的家，司马忍不住有些心酸。`,
			Start:           3102,
			End:             3111,
			Prefix:          "akSrU45XjTGbyT8xVq0v-7708596796: ",
			RetrievalPrefix: "akSrU45XjTGbyT8xVq0v-7708596796: ",
			Keep:            false,
			idx:             2,
		}
		d := Content{Key: "2-1037",
			Value: `500KB_file: ”这个理由是司马将近一年来唯一的借口和理由。

“上次你头被人打破，是因为她吧！还没忘记她吗？”父亲当然了解，不到一年前的那次伤害，对自己儿子造成的伤害。但是，为那样的一个女人显然不值。

“爸！我……她早都是过去了，只是现在……我并不适合找老婆，像现在这样几个星期都不在家呆上一天。找个老婆反而麻烦，要不回头在那边我给你找一个儿媳妇吧！如果你们不嫌她年龄大的话。”听着父亲的话，知道父亲是怕自己还没走出感情的阴影，才会对自己有些担心，于是便半开玩笑的说到。

“行啊！只要你不觉得老，自己个愿意就行，那个时代的儿媳妇可都比现代的要贤慧很多。记着别忘了把我孙子给我们送过来就行。”父亲一听也乐了。自己儿子要在那个时候找媳妇，那生日可都是上个世纪的。

当晚，司马在家里陪父母一起在家吃个团圆饭，很久都没喝酒的父亲也喝了数杯，这一晚父子两人都醉了，谁知道下一次司马再次回来，一家人在一起吃团圆饭是什么时候？    当司马离开家的时候，父亲什么都没说，和母亲在客厅里看着电视，临行时看着有些空荡的家，司马忍不住有些心酸。`,
			Start:           3102,
			End:             3111,
			Prefix:          "500KB_file: ",
			RetrievalPrefix: "500KB_file: ",
			Keep:            false,
			idx:             3,
		}
		e := Content{Key: "2-1038",
			Value: `500KB_file: ”这个理由是司马将近一年来唯一的借口和理由。

“上次你头被人打破，是因为她吧！还没忘记她吗？”父亲当然了解，不到一年前的那次伤害，对自己儿子造成的伤害。但是，为那样的一个女人显然不值。

“爸！我……她早都是过去了，只是现在……我并不适合找老婆，像现在这样几个星期都不在家呆上一天。找个老婆反而麻烦，要不回头在那边我给你找一个儿媳妇吧！如果你们不嫌她年龄大的话。”听着父亲的话，知道父亲是怕自己还没走出感情的阴影，才会对自己有些担心，于是便半开玩笑的说到。

“行啊！只要你不觉得老，自己个愿意就行，那个时代的儿媳妇可都比现代的要贤慧很多。记着别忘了把我孙子给我们送过来就行。”父亲一听也乐了。自己儿子要在那个时候找媳妇，那生日可都是上个世纪的。

当晚，司马在家里陪父母一起在家吃个团圆饭，很久都没喝酒的父亲也喝了数杯，这一晚父子两人都醉了，谁知道下一次司马再次回来，一家人在一起吃团圆饭是什么时候？    当司马离开家的时候，父亲什么都没说，和母亲在客厅里看着电视，临行时看着有些空荡的家，司马忍不住有些心酸。`,

			Start:           3102,
			End:             3111,
			Prefix:          "500KB_file: ",
			RetrievalPrefix: "500KB_file: ",
			Keep:            false,
			idx:             4,
		}
		assert.EqualValues(t, []Content{a, b, c, d, e}, linker.Merge(ctx, []Content{a, b, c, d, e}))
	})
}
